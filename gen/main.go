package main

import (
	"reflect"

	"github.com/bluesky-social/indigo/api"
	atproto "github.com/bluesky-social/indigo/api/atproto"
	bsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/events"
	lexutil "github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/indigo/mst"
	"github.com/bluesky-social/indigo/repo"
	cbg "github.com/whyrusleeping/cbor-gen"
)

func main() {
	var typVals []any
	for _, typ := range mst.CBORTypes() {
		typVals = append(typVals, reflect.New(typ).Elem().Interface())
	}
	if err := cbg.WriteMapEncodersToFile("mst/cbor_gen.go", "mst", typVals...); err != nil {
		panic(err)
	}

	if err := cbg.WriteMapEncodersToFile("repo/cbor_gen.go", "repo", repo.SignedCommit{}, repo.UnsignedCommit{}); err != nil {
		panic(err)
	}

	if err := cbg.WriteMapEncodersToFile("api/cbor_gen.go", "api", api.CreateOp{}); err != nil {
		panic(err)
	}

	if err := cbg.WriteMapEncodersToFile("api/bsky/cbor_gen.go", "bsky",
		bsky.FeedPost{}, bsky.FeedRepost{}, bsky.FeedPost_Entity{},
		bsky.FeedPost_ReplyRef{}, bsky.FeedPost_TextSlice{}, bsky.EmbedImages{},
		bsky.EmbedExternal{}, bsky.EmbedExternal_External{},
		bsky.EmbedImages_Image{}, bsky.GraphFollow{}, bsky.ActorProfile{},
		bsky.EmbedRecord{}, bsky.FeedLike{}, bsky.RichtextFacet{},
		bsky.RichtextFacet_ByteSlice{},
		bsky.RichtextFacet_Link{}, bsky.RichtextFacet_Mention{}, bsky.RichtextFacet_Tag{},
		bsky.EmbedRecordWithMedia{},
		bsky.FeedDefs_NotFoundPost{},
		bsky.GraphBlock{},
		bsky.GraphList{},
		bsky.GraphListitem{},
		bsky.FeedGenerator{},
		bsky.GraphListblock{},
		bsky.EmbedImages_AspectRatio{},
		bsky.FeedThreadgate{},
		bsky.FeedThreadgate_ListRule{},
		bsky.FeedThreadgate_MentionRule{},
		bsky.FeedThreadgate_FollowingRule{},
		/*bsky.EmbedImages_View{},
		bsky.EmbedRecord_View{}, bsky.EmbedRecordWithMedia_View{},
		bsky.EmbedExternal_View{}, bsky.EmbedImages_ViewImage{},
		bsky.EmbedExternal_ViewExternal{}, bsky.EmbedRecord_ViewNotFound{},
		bsky.FeedDefs_ThreadViewPost{}, bsky.EmbedRecord_ViewRecord{},
		bsky.FeedDefs_PostView{}, bsky.ActorDefs_ProfileViewBasic{},
		*/
	); err != nil {
		panic(err)
	}

	if err := cbg.WriteMapEncodersToFile("api/atproto/cbor_gen.go", "atproto",
		atproto.RepoStrongRef{},
		atproto.SyncSubscribeRepos_Commit{},
		atproto.SyncSubscribeRepos_Handle{},
		atproto.SyncSubscribeRepos_Info{},
		atproto.SyncSubscribeRepos_Migrate{},
		atproto.SyncSubscribeRepos_RepoOp{},
		atproto.SyncSubscribeRepos_Tombstone{},
		atproto.LabelDefs_SelfLabels{},
		atproto.LabelDefs_SelfLabel{},
		atproto.LabelDefs_Label{},
		atproto.LabelSubscribeLabels_Labels{},
		atproto.LabelSubscribeLabels_Info{},
	); err != nil {
		panic(err)
	}

	if err := cbg.WriteMapEncodersToFile("lex/util/cbor_gen.go", "util", lexutil.CborChecker{}, lexutil.LegacyBlob{}, lexutil.BlobSchema{}); err != nil {
		panic(err)
	}

	if err := cbg.WriteMapEncodersToFile("events/cbor_gen.go", "events", events.EventHeader{}, events.ErrorFrame{}); err != nil {
		panic(err)
	}
}
