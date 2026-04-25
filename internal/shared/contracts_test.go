package shared

import (
	"encoding/json"
	"testing"
)

func TestNormalizeTextKeepsRussianWordsAndDigits(t *testing.T) {
	input := "\u041f\u043e\u043a\u0430\u0436\u0438, \u0432\u044b\u0440\u0443\u0447\u043a\u0443 \u043f\u043e \u0433\u043e\u0440\u043e\u0434\u0430\u043c \u0437\u0430 \u043f\u043e\u0441\u043b\u0435\u0434\u043d\u0438\u0435 7 \u0434\u043d\u0435\u0439!"
	want := "\u043f\u043e\u043a\u0430\u0436\u0438 \u0432\u044b\u0440\u0443\u0447\u043a\u0443 \u043f\u043e \u0433\u043e\u0440\u043e\u0434\u0430\u043c \u0437\u0430 \u043f\u043e\u0441\u043b\u0435\u0434\u043d\u0438\u0435 7 \u0434\u043d\u0435\u0439"

	if got := NormalizeText(input); got != want {
		t.Fatalf("NormalizeText() = %q, want %q", got, want)
	}
}

func TestNormalizeTextReplacesYoWithYe(t *testing.T) {
	input := "\u0401\u0436\u0438\u043a \u0438 \u0451\u043b\u043a\u0430"
	want := "\u0435\u0436\u0438\u043a \u0438 \u0435\u043b\u043a\u0430"

	if got := NormalizeText(input); got != want {
		t.Fatalf("NormalizeText() = %q, want %q", got, want)
	}
}

func TestIntentUnmarshalAcceptsStringFiltersFromLLM(t *testing.T) {
	raw := []byte(`{
		"metric":"price_threshold_share",
		"group_by":"none",
		"filters":["final_price_local > 500"],
		"period":{"label":"последний месяц","from":"","to":"","grain":"day"},
		"confidence":0.91
	}`)

	var intent Intent
	if err := json.Unmarshal(raw, &intent); err != nil {
		t.Fatalf("json.Unmarshal() returned error: %v", err)
	}
	if len(intent.Filters) != 1 {
		t.Fatalf("filters len = %d, want 1", len(intent.Filters))
	}
	if intent.Filters[0].Field != "final_price_local" || intent.Filters[0].Operator != ">" || intent.Filters[0].Value != "500" {
		t.Fatalf("filter = %+v, want parsed final_price_local > 500", intent.Filters[0])
	}
}

func TestIntentUnmarshalAcceptsNumericFilterValueFromLLM(t *testing.T) {
	raw := []byte(`{
		"metric":"price_threshold_share",
		"group_by":"none",
		"filters":[{"field":"final_price_local","operator":">","value":500}],
		"confidence":0.91
	}`)

	var intent Intent
	if err := json.Unmarshal(raw, &intent); err != nil {
		t.Fatalf("json.Unmarshal() returned error: %v", err)
	}
	if len(intent.Filters) != 1 || intent.Filters[0].Value != "500" {
		t.Fatalf("filter = %+v, want numeric value normalized to string 500", intent.Filters)
	}
}
