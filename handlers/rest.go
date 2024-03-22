package handlers

import (
	"github.com/oleiade/reflections"
	"github.com/qoalis/go-micro/micro"
	"github.com/qoalis/go-micro/schema"
	"github.com/qoalis/go-micro/util/errors"
	"github.com/qoalis/go-micro/util/h"
	"github.com/qoalis/go-micro/util/ids"
)

func GetEntityList[T any](c micro.Ctx, paging schema.PagingInput) schema.EntityList[T] {
	db := c.CurrentDB()
	var data []T
	page := 1
	limit := 1000
	if paging.Count > 0 {
		limit = paging.Count
	}
	if paging.Page > 1 {
		page = paging.Page
	}
	q := micro.Query{
		Offset: int64((page - 1) * limit),
		Limit:  int64(limit),
	}
	h.RaiseAny(db.Find(&data, q))
	return schema.EntityList[T]{
		Data: data,
	}
}

func SearchEntity[T any](c micro.Ctx, input schema.FilterInput) schema.EntityList[T] {
	db := c.CurrentDB()
	var data []T
	page := 1
	limit := 1000
	if input.Paging.Count > 0 {
		limit = input.Paging.Count
	}
	if input.Paging.Page > 1 {
		page = input.Paging.Page
	}
	q := micro.Query{
		W:      input.Where,
		Args:   input.Args,
		Offset: int64((page - 1) * limit),
		Limit:  int64(limit),
	}
	h.RaiseAny(db.Find(&data, q))
	return schema.EntityList[T]{
		Data: data,
	}
}

func CreateEntity[T any](c micro.Ctx, input any, l ...micro.EntityListener[T]) T {
	db := c.CurrentDB()
	var entity T
	h.RaiseAny(h.CopyAllFields(&entity, input, true))
	prefix := h.F(reflections.GetFieldTag(entity, "Id", "prefix"))
	h.RaiseIf(h.IsStrEmpty(prefix), errors.Technical("entity_missing_id_prefix"))
	h.RaiseAny(reflections.SetField(&entity, "Id", ids.NewIdPtr(prefix)))
	if len(l) > 0 {
		h.RaiseAny(l[0].PreCreate(&entity))
	}
	h.RaiseAny(db.Create(&entity))
	return entity
}

func UpdateEntity[T any](c micro.Ctx, input any, l ...micro.EntityListener[T]) T {
	db := c.CurrentDB()
	id := h.UnwrapStr(h.F(reflections.GetField(input, "Id")))
	var entity T
	found := h.F(db.First(&entity, micro.Query{
		W:    "id = ?",
		Args: []any{id},
	}))
	h.RaiseIf(!found, errors.Functional("entity_not_found"))
	h.RaiseAny(h.CopyAllFields(&entity, input, true))
	if len(l) > 0 {
		h.RaiseAny(l[0].PreUpdate(&entity))
	}
	h.RaiseAny(db.Save(&entity))
	return entity
}

func DeleteEntity[T any](c micro.Ctx, input schema.IdModel) schema.IdModel {
	db := c.CurrentDB()
	var entity T
	_, err := db.Delete(entity, micro.Query{
		W:    "id = ?",
		Args: []any{*input.Id},
	})
	h.RaiseAny(err)
	return input
}
