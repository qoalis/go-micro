package micro

import (
	"github.com/qoalis/go-micro/util/errors"
	"github.com/qoalis/go-micro/util/h"
)

type RepoHooks[T any] struct {
	PreCreate func(e *T)
}

type EntityRepo[T any] interface {
	Create(Ctx, *T) error
	CreateAll(Ctx, []*T) error
	UpdateAll(Ctx, []*T) error
	Update(Ctx, *T) error
	DeleteById(Ctx, string) error
	Merge(Ctx, string, func(target *T)) (*T, error)
	FindAll(Ctx) ([]*T, error)
	FindAllSorted(Ctx, string) ([]*T, error)
	FindById(Ctx, string) (*T, error)
	FindByIds(ctx Ctx, values []string) ([]*T, error)
	CountAll(Ctx) (int64, error)
}

type EntityRepoImpl[T any] interface {
	EntityRepo[T]
	Query(Ctx, interface{}, string, ...interface{}) error
	Raw(Ctx, string, ...interface{}) error
	Patch(Ctx, string, map[string]any) error
	FindBySorted(ctx Ctx, sortBy string, where string, args ...interface{}) ([]*T, error)
	FindBy(Ctx, string, ...interface{}) ([]*T, error)
	FindByInto(Ctx, any, string, ...interface{}) error
	FirstBy(Ctx, string, ...interface{}) (*T, error)
	CountBy(Ctx, string, ...interface{}) (int64, error)
	ExistsBy(Ctx, string, ...interface{}) (bool, error)
	DeleteBy(Ctx, string, ...interface{}) error
}

type LinkedEntityRepo[T any] interface {
	Create(*T) error
	CreateAll([]*T) error
	UpdateAll([]*T) error
	Update(*T) error
	DeleteById(string) error
	Merge(string, func(*T)) (*T, error)
	FindAll() ([]*T, error)
	FindAllSorted(string) ([]*T, error)
	FindById(string) (*T, error)
	FindBy(string, ...interface{}) ([]*T, error)
	FirstBy(query string, args ...interface{}) (*T, error)
	ExistsById(string) (bool, error)
	FindByIds([]string) ([]*T, error)
	CountAll() (int64, error)
}

type LinkedEntityRepoImpl[T any] interface {
	LinkedEntityRepo[T]
	Query(interface{}, string, ...interface{}) error
	Raw(string, ...interface{}) error
	Patch(string, map[string]any) error
	FindBySorted(sortBy string, where string, args ...interface{}) ([]*T, error)
	FindByInto(any, string, ...interface{}) error
	FirstBy(string, ...interface{}) (*T, error)
	CountBy(string, ...interface{}) (int64, error)
	ExistsBy(string, ...interface{}) (bool, error)
	DeleteBy(string, ...interface{}) error
}

type entityRepoImpl[T any] struct {
	EntityRepoImpl[T]
	hooks RepoHooks[T]
}

type linkedEntityRepoImpl[T any] struct {
	LinkedEntityRepoImpl[T]
	db    DataSource
	hooks RepoHooks[T]
}

func NewRepoImpl[T any](preCreate func(e *T)) EntityRepoImpl[T] {
	return entityRepoImpl[T]{
		hooks: RepoHooks[T]{
			PreCreate: preCreate,
		},
	}
}

func NewLinkedEntityRepo[T any](preCreate func(e *T), db DataSource) LinkedEntityRepoImpl[T] {
	return linkedEntityRepoImpl[T]{
		db: db,
		hooks: RepoHooks[T]{
			PreCreate: preCreate,
		},
	}
}

// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// linkedEntityRepoImpl
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

func (r linkedEntityRepoImpl[T]) CreateAll(entities []*T) error {
	for _, e := range entities {
		r.hooks.PreCreate(e)
	}
	return _createAll[T](r.db, entities)
}

func (r linkedEntityRepoImpl[T]) Create(record *T) error {
	r.hooks.PreCreate(record)
	return _create[T](r.db, record)
}

func (r linkedEntityRepoImpl[T]) Update(data *T) error {
	return _update[T](r.db, data)
}

func (r linkedEntityRepoImpl[T]) UpdateAll(data []*T) error {
	return _updateAll[T](r.db, data)
}

func (r linkedEntityRepoImpl[T]) DeleteBy(where string, args ...interface{}) error {
	return _deleteBy[T](r.db, where, args...)
}

func (r linkedEntityRepoImpl[T]) DeleteById(value string) error {
	return _deleteById[T](r.db, value)
}

func (r linkedEntityRepoImpl[T]) Patch(id string, value map[string]interface{}) error {
	return _patch[T](r.db, id, value)
}

func (r linkedEntityRepoImpl[T]) Merge(id string, merger func(target *T)) (*T, error) {
	return _merge[T](r.db, id, merger)
}

func (r linkedEntityRepoImpl[T]) ExistsBy(where string, args ...interface{}) (bool, error) {
	return _existsBy[T](r.db, where, args...)
}

func (r linkedEntityRepoImpl[T]) FindAll() ([]*T, error) {
	return _findAll[T](r.db)
}

func (r linkedEntityRepoImpl[T]) FindAllSorted(orderBy string) ([]*T, error) {
	return _findAllSorted[T](r.db, orderBy)
}

func (r linkedEntityRepoImpl[T]) FindByInto(target any, where string, args ...interface{}) error {
	return _findByInto[T](r.db, target, where, args...)
}

func (r linkedEntityRepoImpl[T]) FindBy(where string, args ...interface{}) ([]*T, error) {
	return _findBy[T](r.db, where, args...)
}

func (r linkedEntityRepoImpl[T]) FindBySorted(sort string, where string, args ...interface{}) ([]*T, error) {
	return _findBySorted[T](r.db, sort, where, args...)
}

func (r linkedEntityRepoImpl[T]) FindById(id string) (*T, error) {
	return _firstBy[T](r.db, "id=?", id)
}

func (r linkedEntityRepoImpl[T]) ExistsById(id string) (bool, error) {
	count, err := _countBy[T](r.db, "id=?", id)
	return count > 0, err
}

func (r linkedEntityRepoImpl[T]) FindByIds(ids []string) ([]*T, error) {
	return _findBy[T](r.db, "id in (?)", ids)
}

func (r linkedEntityRepoImpl[T]) FirstBy(where string, args ...interface{}) (*T, error) {
	return _firstBy[T](r.db, where, args...)
}

func (r linkedEntityRepoImpl[T]) CountBy(where string, args ...interface{}) (int64, error) {
	return _countBy[T](r.db, where, args...)
}

func (r linkedEntityRepoImpl[T]) CountAll() (int64, error) {
	return _countAll[T](r.db)
}

func (r linkedEntityRepoImpl[T]) Query(target interface{}, raw string, args ...interface{}) error {
	return _query(r.db, target, raw, args...)
}

func (r linkedEntityRepoImpl[T]) Raw(raw string, args ...interface{}) error {
	return _raw[T](r.db, raw, args...)
}

// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// entityRepoImpl
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

func (r entityRepoImpl[T]) CreateAll(ctx Ctx, entities []*T) error {
	for _, e := range entities {
		r.hooks.PreCreate(e)
	}
	return _createAll[T](ctx.db, entities)
}

func (r entityRepoImpl[T]) Create(ctx Ctx, record *T) error {
	r.hooks.PreCreate(record)
	return _create[T](ctx.db, record)
}

func (r entityRepoImpl[T]) Update(ctx Ctx, data *T) error {
	return _update[T](ctx.db, data)
}

func (r entityRepoImpl[T]) UpdateAll(ctx Ctx, data []*T) error {
	return _updateAll[T](ctx.db, data)
}

func (r entityRepoImpl[T]) DeleteBy(ctx Ctx, where string, args ...interface{}) error {
	return _deleteBy[T](ctx.db, where, args...)
}

func (r entityRepoImpl[T]) DeleteById(ctx Ctx, value string) error {
	return _deleteById[T](ctx.db, value)
}

func (r entityRepoImpl[T]) Patch(ctx Ctx, id string, value map[string]interface{}) error {
	return _patch[T](ctx.db, id, value)
}

func (r entityRepoImpl[T]) Merge(ctx Ctx, id string, merger func(target *T)) (*T, error) {
	return _merge[T](ctx.db, id, merger)
}

func (r entityRepoImpl[T]) ExistsBy(ctx Ctx, where string, args ...interface{}) (bool, error) {
	return _existsBy[T](ctx.db, where, args...)
}

func (r entityRepoImpl[T]) FindAll(ctx Ctx) ([]*T, error) {
	return _findAll[T](ctx.db)
}

func (r entityRepoImpl[T]) FindAllSorted(ctx Ctx, orderBy string) ([]*T, error) {
	return _findAllSorted[T](ctx.db, orderBy)
}

func (r entityRepoImpl[T]) FindByInto(ctx Ctx, target any, where string, args ...interface{}) error {
	return _findByInto[T](ctx.db, target, where, args...)
}

func (r entityRepoImpl[T]) FindBy(ctx Ctx, where string, args ...interface{}) ([]*T, error) {
	return _findBy[T](ctx.db, where, args...)
}

func (r entityRepoImpl[T]) FindBySorted(ctx Ctx, sort string, where string, args ...interface{}) ([]*T, error) {
	return _findBySorted[T](ctx.db, sort, where, args...)
}

func (r entityRepoImpl[T]) FindById(ctx Ctx, id string) (*T, error) {
	return _firstBy[T](ctx.db, "id=?", id)
}

func (r entityRepoImpl[T]) FindByIds(ctx Ctx, ids []string) ([]*T, error) {
	return _findBy[T](ctx.db, "id in (?)", ids)
}

func (r entityRepoImpl[T]) FirstBy(ctx Ctx, where string, args ...interface{}) (*T, error) {
	return _firstBy[T](ctx.db, where, args...)
}

func (r entityRepoImpl[T]) CountBy(ctx Ctx, where string, args ...interface{}) (int64, error) {
	return _countBy[T](ctx.db, where, args...)
}

func (r entityRepoImpl[T]) CountAll(ctx Ctx) (int64, error) {
	return _countAll[T](ctx.db)
}

func (r entityRepoImpl[T]) Query(ctx Ctx, target interface{}, raw string, args ...interface{}) error {
	return _query(ctx.db, target, raw, args...)
}

func (r entityRepoImpl[T]) Raw(ctx Ctx, raw string, args ...interface{}) error {
	return _raw[T](ctx.db, raw, args...)
}

// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// db impl
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

func _createAll[T any](db DataSource, entities []*T) error {
	return db.Create(entities)
}

func _create[T any](db DataSource, record *T) error {
	return db.Create(&record)
}

func _update[T any](db DataSource, data *T) error {
	return db.Save(&data)
}

func _updateAll[T any](db DataSource, data []*T) error {
	return db.Save(&data)
}

func _deleteBy[T any](db DataSource, where string, args ...interface{}) error {
	var model T
	_, err := db.Delete(&model, Query{W: where, Args: args})
	return err
}

func _deleteById[T any](db DataSource, value string) error {
	return _deleteBy[T](db, "id=?", value)
}

func _patch[T any](db DataSource, id string, value map[string]interface{}) error {
	var model T
	_, err := db.Patch(model, id, value)
	return err
}

func _merge[T any](db DataSource, id string, merger func(target *T)) (*T, error) {
	loaded, err := _findById[T](db, id)
	if err != nil {
		return nil, errors.ResourceNotFound("missing_entity")
	}
	beforeMerge := *loaded
	merger(loaded)
	if &beforeMerge != loaded {
		err = db.Save(loaded)
	}
	return loaded, err
}

func _existsBy[T any](db DataSource, where string, args ...interface{}) (bool, error) {
	var model T
	return db.Exists(model, Query{W: where, Args: args})
}

func _findAll[T any](db DataSource) ([]*T, error) {
	var model []*T
	err := db.Find(&model, Query{})
	return model, err
}

func _findAllSorted[T any](db DataSource, orderBy string) ([]*T, error) {
	var model []*T
	err := db.Find(&model, Query{Sort: orderBy})
	return model, err
}

func _findByInto[T any](db DataSource, target any, where string, args ...interface{}) error {
	var model []*T
	err := db.Find(&target, Query{W: where, Args: args, Model: model})
	return err
}

func _findBy[T any](db DataSource, where string, args ...interface{}) ([]*T, error) {
	var model []*T
	err := db.Find(&model, Query{W: where, Args: args})
	return model, err
}

func _findBySorted[T any](db DataSource, sort string, where string, args ...interface{}) ([]*T, error) {
	var model []*T
	err := db.Find(&model, Query{W: where, Args: args, Sort: sort})
	return model, err
}

func _findById[T any](db DataSource, id string) (*T, error) {
	return _firstBy[T](db, "id=?", id)
}

func _findByIds[T any](db DataSource, ids []string) ([]*T, error) {
	return _findBy[T](db, "id in (?)", ids)
}

func _firstBy[T any](db DataSource, where string, args ...interface{}) (*T, error) {
	var model T
	found, err := db.First(&model, Query{W: where, Args: args})
	if !found || err != nil {
		return nil, err
	}
	return &model, err
}

func _countBy[T any](db DataSource, where string, args ...interface{}) (int64, error) {
	var model T
	return db.Count(model, Query{W: where, Args: args})
}

func _countAll[T any](db DataSource) (int64, error) {
	var model T
	return db.Count(model, Query{})
}

func _query(db DataSource, target interface{}, raw string, args ...interface{}) error {
	if !h.IsPointer(target) {
		panic("target must be a pointer")
	}
	return db.Find(target, Query{
		Raw:  raw,
		Args: args,
	})
}

func _raw[T any](db DataSource, raw string, args ...interface{}) error {
	var m = new(T)
	_, err := db.Execute(m, Query{Model: m, Raw: raw, Args: args})
	return err
}
