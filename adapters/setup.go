package adapters

import (
	"fmt"
	"github.com/joho/godotenv"
	"github.com/nicksnyder/go-i18n/v2/i18n"
	"github.com/pelletier/go-toml/v2"
	"github.com/qoalis/go-micro/micro"
	"github.com/qoalis/go-micro/util/h"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"github.com/thoas/go-funk"
	"golang.org/x/text/language"
	"os"
	"strings"
)

func NewApp(name string, version string, cfg micro.Cfg) *micro.App {
	isProduction := os.Getenv("GO_ENV") == "production"
	if !isProduction {
		err := godotenv.Load()
		if err != nil {
			log.Warn("unable to loading .env file")
		}
		micro.Set(micro.InsecureJwtDev, os.Getenv(micro.InsecureJwtDev))
	}

	env := &micro.Env{
		Production: isProduction,
		AppName:    name,
		AppVersion: version,
	}

	env.MultiTenant = cfg.MultiTenant
	env.ServerPort = h.ToInt(h.GetEnvOrDefault("PORT", "8080"))
	setupLocales(env, cfg)
	prepareMultiTenancy(env, cfg)
	setupDatabase(env, cfg)
	setupScheduler(env)
	setupMailer(env)
	setupNotifications(env)
	setupTokenProvider(env)
	setupRedis(env, cfg)
	router := setupRouter(env, cfg)

	// configure locales if any
	app := &micro.App{
		Name:              name,
		Version:           version,
		Env:               env,
		ShutdownListeners: []func(){},
		Router:            router,
	}

	return app

}

func setupLocales(env *micro.Env, cfg micro.Cfg) {
	if cfg.AvailableLocales == nil && cfg.DefaultLocale == "" {
		return
	}
	exists, localesFS := h.CheckFsFolder(cfg.FS, "locales")
	if !exists {
		log.Error("no config/locales is missing, skipping")
		return
	}
	bundle := i18n.NewBundle(language.French)
	bundle.RegisterUnmarshalFunc("toml", toml.Unmarshal)

	locales := cfg.AvailableLocales
	if (locales == nil || len(locales) == 0) && cfg.DefaultLocale != "" {
		locales = []string{cfg.DefaultLocale}
	}

	for _, lang := range locales {
		_, err := bundle.LoadMessageFileFS(localesFS, fmt.Sprintf("locale.%s.toml", lang))
		if err != nil {
			panic(err)
		}
	}
	localizer := i18n.NewLocalizer(bundle, locales...)
	log.Infof("%s locales loaded", strings.Join(locales, ","))
	env.Localizer = localizer

}

func getInitialTenants() []string {
	tenants := strings.Split(h.RequireEnv(micro.DatabaseInitialTenants), ",")
	return funk.Map(tenants, func(tenant string) string {
		parts := strings.Split(tenant, "|")
		return parts[0]
	}).([]string)
}

func prepareMultiTenancy(env *micro.Env, cfg micro.Cfg) {
	var tenantLoader micro.TenantLoader
	if cfg.MultiTenant {
		defaultTenants := getInitialTenants()
		tenantLoader = micro.NewFixedTenantLoader(defaultTenants)
	} else {
		tenantLoader = micro.NewFixedTenantLoader([]string{micro.DefaultTenantId})
	}
	env.TenantLoader = tenantLoader
}

func setupDatabase(env *micro.Env, cfg micro.Cfg) {
	databaseUrl := h.GetEnv(micro.DatabaseUrl)
	if databaseUrl == "" {
		return
	}
	log.Infof("env.%s detected, configuring database", micro.DatabaseUrl)
	/*exists, migrationsFS := h.CheckFsFolder(cfg.FS, "db/migrations")
	  if !exists {
	  	log.Error("no config/db/migrations found, skipping")
	  	return
	  }*/
	if env.TenantLoader == nil {
		env.TenantLoader = micro.NewFixedTenantLoader([]string{micro.DefaultTenantId})
	}
	tenants := env.TenantLoader.GetTenant()
	links := map[string]micro.DataSource{}
	env.DataSources = links

	//migrationsTable := cfg.TablePrefix + micro.DefaultMigrationsTable
	tenants = append(tenants, micro.DefaultTenantId)

	if cfg.MultiTenant {
		for _, tenant := range tenants {
			if _, ok := links[tenant]; ok {
				continue
			}
			links[tenant] = NewGormAdapter(databaseUrl, tenant)
			/*if tenant == micro.DefaultTenantId {
			  	links[tenant].Migrate(migrationsFS, "shared", migrationsTable)
			  } else {
			  	links[tenant].Migrate(migrationsFS, "tenant", migrationsTable)
			  }*/
		}
	} else {
		for _, tenant := range tenants {
			if _, ok := links[tenant]; ok {
				continue
			}
			links[tenant] = NewGormAdapter(databaseUrl, tenant)
			//links[tenant].Migrate(migrationsFS, ".", migrationsTable)
		}
	}

}

func setupScheduler(env *micro.Env) {
	env.Scheduler = NewGoCronAdapter(env, env.TenantLoader)
}

func setupMailer(env *micro.Env) {
	config := h.GetEnv(micro.EmailSender, "MAILER")
	if config == "" {
		return
	}
	log.Infof("env.%s found, configuring mailer", micro.EmailSender)
	var mailer micro.Mailer
	mailerConfig := strings.Split(config, "://")
	if mailerConfig[0] == "sendgrid" {
		mailer = NewSendGridEmailSender(mailerConfig[1])
	} else if mailerConfig[0] == "fake" {
		mailer = NewFakeEmailSender()
	} else {
		log.Fatalf("mailer provider not supported: %s", mailerConfig)
	}
	env.Mailer = mailer
}

func setupNotifications(env *micro.Env) {

	config := h.GetEnv(micro.NotificationSender)
	if config == "" {
		return
	}

	log.Infof("env.%s found, configuring...", micro.NotificationSender)
	var service micro.NotificationService

	if strings.Contains(config, "discord.com") {
		service = NewDiscordClient(config)
	} else if strings.Contains(config, "noop") {
		service = micro.NewNoopNotificationService()
	} else {
		log.Fatalf("notifications manager provider not supported: %s", config)
	}

	env.Notifier = service

}

func setupTokenProvider(env *micro.Env) {
	secret := h.GetEnv(micro.ServerToken)
	if secret == "" {
		return
	}
	log.Infof("env.%s detected, configuring token provider", micro.ServerToken)
	env.TokenProvider = micro.NewJwtTokenProvider(secret)
}

func setupRedis(env *micro.Env, cfg micro.Cfg) {
	redisUrl := h.GetEnv(micro.RedisUrl)
	if redisUrl == "" {
		return
	}
	log.Infof("env.%s detected, configuring redis client", micro.RedisUrl)
	opts, err := redis.ParseURL(redisUrl)
	if err != nil {
		log.Fatalf("error configuring redis: %s", err)
	}
	rdb := redis.NewClient(opts)
	env.RedisClient = rdb
	if cfg.EnableDiscovery {
		env.DiscoverySericeName = micro.DiscoveryServicePrefix + env.AppName
		hostname := h.GetEnv("APP_PRIVATE_DOMAIN", "APP_DOMAIN", "RAILWAY_PRIVATE_DOMAIN", "RAILWAY_PUBLIC_DOMAIN")
		if hostname == "" {
			hostname = "localhost"
		}
		env.DiscoveryServiceUrl = fmt.Sprintf("%s:%d", hostname, env.ServerPort)
		log.Infof("DISCROVERY_URL set to %s", env.DiscoveryServiceUrl)
	}
}

func setupRouter(env *micro.Env, cfg micro.Cfg) micro.Router {

	if cfg.DisableRouter {
		return nil
	}
	corsEnabled := true
	if cfg.CorsDisabled {
		corsEnabled = false
	} else if h.GetEnv("CORS_DISABLED") == "true" {
		corsEnabled = false
	}

	return NewEchoAdapter(
		env,
		micro.RouterConfig{
			Cors:                       corsEnabled,
			SentryDsn:                  h.GetEnv("SENTRY_DSN"),
			RemoveTrailSlash:           true,
			BasePath:                   cfg.BasePath,
			DisableImplicitTransaction: cfg.DisableImplicitTransaction,
			BodyLimit:                  "2M",
			SwaggerSpec:                cfg.SwaggerSpec,
			Production:                 env.Production,
			TokenProvider:              env.TokenProvider,
			DisableJwtFilter:           cfg.DisableJwtFilter,
			MultiTenant:                cfg.MultiTenant,
		})

}
