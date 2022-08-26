package load

import (
	"context"
	"database/sql/driver"

	"github.com/yansal/sql/build"
)

type PreloadModel interface {
	GetPreloadBindValue(string) any
	GetPreloadDestIdent(string) string
	GetPreloadDestValue(string, any) any
	SetPreloadDest(string, any)
}

func PreloadSlice[
	PreloadDest any,
	PreloadSrc any,
	PtrToPreloadDest interface {
		*PreloadDest
		Model
	},
	PtrToPreloadSrc interface {
		*PreloadSrc
		PreloadModel
	},
](ctx context.Context, db Querier, srcs []PreloadSrc, destname string) error {
	if len(srcs) == 0 {
		return nil
	}
	bindvaluemap := make(map[any]struct{})
	for i := range srcs {
		var (
			srcptr    PtrToPreloadSrc = &srcs[i]
			bindvalue                 = srcptr.GetPreloadBindValue(destname)
		)
		if valuer, ok := bindvalue.(driver.Valuer); ok {
			value, err := valuer.Value()
			if err != nil {
				return err
			}
			if value == nil {
				continue
			}
			bindvalue = value
		}
		bindvaluemap[bindvalue] = struct{}{}
	}
	if len(bindvaluemap) == 0 {
		return nil
	}
	bindvalues := make([]any, 0, len(bindvaluemap))
	for v := range bindvaluemap {
		bindvalues = append(bindvalues, v)
	}

	var (
		srcmodel PtrToPreloadSrc
		where    = build.Ident(srcmodel.GetPreloadDestIdent(destname)).In(build.Bind(bindvalues))
	)
	dests, err := Find[PreloadDest, PtrToPreloadDest](ctx, db, WithWhere(where))
	if err != nil {
		return err
	}

	destmap := make(map[any][]PreloadDest)
	for i := range dests {
		destvalue := srcmodel.GetPreloadDestValue(destname, dests[i])
		if valuer, ok := destvalue.(driver.Valuer); ok {
			value, err := valuer.Value()
			if err != nil {
				return err
			}
			if value == nil {
				continue
			}
			destvalue = value
		}
		destmap[destvalue] = append(destmap[destvalue], dests[i])
	}
	for i := range srcs {
		var (
			srcptr    PtrToPreloadSrc = &srcs[i]
			bindvalue                 = srcptr.GetPreloadBindValue(destname)
		)
		if valuer, ok := bindvalue.(driver.Valuer); ok {
			value, err := valuer.Value()
			if err != nil {
				return err
			}
			if value == nil {
				continue
			}
			bindvalue = value
		}
		if v := destmap[bindvalue]; len(v) > 0 {
			srcptr.SetPreloadDest(destname, v)
		}
	}
	return nil
}

func PreloadPtr[
	PreloadDest any,
	PtrToPreloadDest interface {
		*PreloadDest
		Model
	},
	PreloadSrc any,
	PtrToPreloadSrc interface {
		*PreloadSrc
		PreloadModel
	},
](ctx context.Context, db Querier, srcptr PtrToPreloadSrc, destname string) error {
	srcs := []PreloadSrc{*srcptr}
	if err := PreloadSlice[
		PreloadDest,
		PreloadSrc,
		PtrToPreloadDest,
		PtrToPreloadSrc,
	](ctx, db, srcs, destname); err != nil {
		return err
	}
	*srcptr = srcs[0]
	return nil
}
