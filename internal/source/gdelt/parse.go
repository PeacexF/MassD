package gdelt

import (
	"strconv"
	"strings"
	"time"

	"github.com/PeacexF/MassD/internal/record"
)

// Column positions in the GDELT 2.0 export and mentions tables. The files are
// tab separated with no header and no quoting.
const (
	evGlobalEventID = 0
	evDay           = 1
	evMonthYear     = 2
	evYear          = 3
	evFractionDate  = 4
	evActor1Code    = 5
	evActor1Name    = 6
	evActor1Country = 7
	evActor1Group   = 8
	evActor1Type1   = 12
	evActor2Code    = 15
	evActor2Name    = 16
	evActor2Country = 17
	evActor2Group   = 18
	evActor2Type1   = 22
	evIsRootEvent   = 25
	evEventCode     = 26
	evEventBaseCode = 27
	evEventRootCode = 28
	evQuadClass     = 29
	evGoldstein     = 30
	evNumMentions   = 31
	evNumSources    = 32
	evNumArticles   = 33
	evAvgTone       = 34
	evA1GeoCountry  = 37
	evA1GeoLat      = 40
	evA1GeoLong     = 41
	evA2GeoCountry  = 45
	evA2GeoLat      = 48
	evA2GeoLong     = 49
	evActGeoType    = 51
	evActGeoName    = 52
	evActGeoCountry = 53
	evActGeoADM1    = 54
	evActGeoLat     = 56
	evActGeoLong    = 57
	evActGeoFeature = 58
	evDateAdded     = 59
	evSourceURL     = 60
	evMinFields     = 61
)

const (
	mnGlobalEventID = 0
	mnEventTimeDate = 1
	mnMentionTime   = 2
	mnMentionType   = 3
	mnSourceName    = 4
	mnIdentifier    = 5
	mnSentenceID    = 6
	mnInRawText     = 10
	mnConfidence    = 11
	mnDocLen        = 12
	mnDocTone       = 13
	mnMinFields     = 14
)

func field(cols []string, i int) string {
	if i < len(cols) {
		return strings.TrimSpace(cols[i])
	}
	return ""
}

func text(cols []string, i int) any {
	if v := field(cols, i); v != "" {
		return v
	}
	return nil
}

func integer(cols []string, i int) any {
	v := field(cols, i)
	if v == "" {
		return nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return nil
	}
	return n
}

func decimal(cols []string, i int) any {
	v := field(cols, i)
	if v == "" {
		return nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return nil
	}
	return f
}

// stampTime converts GDELT's YYYYMMDDHHMMSS integers into a sortable timestamp.
func stampTime(cols []string, i int) any {
	v := field(cols, i)
	if len(v) != 14 {
		return nil
	}
	t, err := time.Parse("20060102150405", v)
	if err != nil {
		return nil
	}
	return t.UTC().Format(time.RFC3339)
}

func eventRecord(cols []string, file, now string, runID int64) (record.Record, bool) {
	if len(cols) < evMinFields || field(cols, evGlobalEventID) == "" {
		return record.Record{}, false
	}
	id := integer(cols, evGlobalEventID)
	if id == nil {
		return record.Record{}, false
	}

	return record.New("gdelt_events").
		Set("global_event_id", id).
		Set("day", integer(cols, evDay)).
		Set("month_year", integer(cols, evMonthYear)).
		Set("year", integer(cols, evYear)).
		Set("fraction_date", decimal(cols, evFractionDate)).
		Set("actor1_code", text(cols, evActor1Code)).
		Set("actor1_name", text(cols, evActor1Name)).
		Set("actor1_country", text(cols, evActor1Country)).
		Set("actor1_known_group", text(cols, evActor1Group)).
		Set("actor1_type1", text(cols, evActor1Type1)).
		Set("actor2_code", text(cols, evActor2Code)).
		Set("actor2_name", text(cols, evActor2Name)).
		Set("actor2_country", text(cols, evActor2Country)).
		Set("actor2_known_group", text(cols, evActor2Group)).
		Set("actor2_type1", text(cols, evActor2Type1)).
		Set("is_root_event", integer(cols, evIsRootEvent)).
		Set("event_code", text(cols, evEventCode)).
		Set("event_base_code", text(cols, evEventBaseCode)).
		Set("event_root_code", text(cols, evEventRootCode)).
		Set("quad_class", integer(cols, evQuadClass)).
		Set("goldstein_scale", decimal(cols, evGoldstein)).
		Set("num_mentions", integer(cols, evNumMentions)).
		Set("num_sources", integer(cols, evNumSources)).
		Set("num_articles", integer(cols, evNumArticles)).
		Set("avg_tone", decimal(cols, evAvgTone)).
		Set("actor1_geo_country", text(cols, evA1GeoCountry)).
		Set("actor1_geo_lat", decimal(cols, evA1GeoLat)).
		Set("actor1_geo_long", decimal(cols, evA1GeoLong)).
		Set("actor2_geo_country", text(cols, evA2GeoCountry)).
		Set("actor2_geo_lat", decimal(cols, evA2GeoLat)).
		Set("actor2_geo_long", decimal(cols, evA2GeoLong)).
		Set("action_geo_type", integer(cols, evActGeoType)).
		Set("action_geo_fullname", text(cols, evActGeoName)).
		Set("action_geo_country", text(cols, evActGeoCountry)).
		Set("action_geo_adm1", text(cols, evActGeoADM1)).
		Set("action_geo_lat", decimal(cols, evActGeoLat)).
		Set("action_geo_long", decimal(cols, evActGeoLong)).
		Set("action_geo_feature_id", text(cols, evActGeoFeature)).
		Set("date_added", integer(cols, evDateAdded)).
		Set("date_added_at", stampTime(cols, evDateAdded)).
		Set("source_url", text(cols, evSourceURL)).
		Set("source_file", file).
		Set("collected_at", now).
		Set("run_id", runID).
		OnConflict(record.Ignore), true
}

func mentionRecord(cols []string, file, now string, runID int64) (record.Record, bool) {
	if len(cols) < mnMinFields || field(cols, mnGlobalEventID) == "" || field(cols, mnIdentifier) == "" {
		return record.Record{}, false
	}
	id := integer(cols, mnGlobalEventID)
	if id == nil {
		return record.Record{}, false
	}

	return record.New("gdelt_mentions").
		Set("global_event_id", id).
		Set("event_time_date", integer(cols, mnEventTimeDate)).
		Set("mention_time_date", integer(cols, mnMentionTime)).
		Set("mention_time_at", stampTime(cols, mnMentionTime)).
		Set("mention_type", integer(cols, mnMentionType)).
		Set("mention_source_name", text(cols, mnSourceName)).
		Set("mention_identifier", field(cols, mnIdentifier)).
		Set("sentence_id", integer(cols, mnSentenceID)).
		Set("in_raw_text", integer(cols, mnInRawText)).
		Set("confidence", integer(cols, mnConfidence)).
		Set("mention_doc_len", integer(cols, mnDocLen)).
		Set("mention_doc_tone", decimal(cols, mnDocTone)).
		Set("source_file", file).
		Set("collected_at", now).
		Set("run_id", runID).
		OnConflict(record.Ignore), true
}
