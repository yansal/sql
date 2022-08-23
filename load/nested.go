package load

import (
	"context"
	"fmt"
	"strings"
)

type NestedModel interface {
	GetField(string) any
	SetField(string, any)
}

func PreloadSliceNested[
	Nested any,
	PreloadDest any,
	PreloadSrc any,
	PtrToNested interface {
		*Nested
		PreloadModel
	},
	PtrToPreloadDest interface {
		*PreloadDest
		Model
	},
	PtrToPreloadSrc interface {
		*PreloadSrc
		NestedModel
	},
](ctx context.Context, db Querier, srcs []PreloadSrc, destname string) error {
	split := strings.Split(destname, ".")
	if len(split) != 2 {
		panic(fmt.Sprintf("expected 1 nested preload destname, got %q", destname))
	}

	var (
		allnested []Nested
		indexes   []struct{ i, j int }
	)
	for i := range srcs {
		var ptr PtrToPreloadSrc = &srcs[i]
		nested := ptr.GetField(split[0]).([]Nested)
		indexes = append(indexes, struct{ i, j int }{i: len(allnested), j: len(allnested) + len(nested)})
		allnested = append(allnested, nested...)
	}
	if err := PreloadSlice[
		PreloadDest,
		Nested,
		PtrToPreloadDest,
		PtrToNested,
	](ctx, db, allnested, split[1]); err != nil {
		return err
	}
	for i := range srcs {
		var ptr PtrToPreloadSrc = &srcs[i]
		ptr.SetField(split[0], allnested[indexes[i].i:indexes[i].j])
	}
	return nil
}
