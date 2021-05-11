package csv_test

import (
	"strings"
	"testing"

	"github.com/AleksandrMac/csv_query/pkg/csv"
	"github.com/stretchr/testify/assert"
)

var (
	firstRow string = "iso_code,continent,location,date,total_cases,new_cases,new_cases_smoothed,total_deaths,new_deaths,new_deaths_smoothed,total_cases_per_million,new_cases_per_million,new_cases_smoothed_per_million,total_deaths_per_million,new_deaths_per_million,new_deaths_smoothed_per_million,reproduction_rate,icu_patients,icu_patients_per_million,hosp_patients,hosp_patients_per_million,weekly_icu_admissions,weekly_icu_admissions_per_million,weekly_hosp_admissions,weekly_hosp_admissions_per_million,new_tests,total_tests,total_tests_per_thousand,new_tests_per_thousand,new_tests_smoothed,new_tests_smoothed_per_thousand,positive_rate,tests_per_case,tests_units,total_vaccinations,people_vaccinated,people_fully_vaccinated,new_vaccinations,new_vaccinations_smoothed,total_vaccinations_per_hundred,people_vaccinated_per_hundred,people_fully_vaccinated_per_hundred,new_vaccinations_smoothed_per_million,stringency_index,population,population_density,median_age,aged_65_older,aged_70_older,gdp_per_capita,extreme_poverty,cardiovasc_death_rate,diabetes_prevalence,female_smokers,male_smokers,handwashing_facilities,hospital_beds_per_thousand,life_expectancy,human_development_index"
	data     string = `AFG,Asia,Afghanistan,2020-02-24,1.0,1.0,,,,,0.026,0.026,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,8.33,38928341.0,54.422,18.6,2.581,1.337,1803.987,,597.029,9.59,,,37.746,0.5,64.83,0.511\n
AFG,Asia,Afghanistan,2020-02-25,1.0,0.0,,,,,0.026,0.0,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,8.33,38928341.0,54.422,18.6,2.581,1.337,1803.987,,597.029,9.59,,,37.746,0.5,64.83,0.511\n
AFG,Asia,Afghanistan,2020-02-26,1.0,0.0,,,,,0.026,0.0,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,8.33,38928341.0,54.422,18.6,2.581,1.337,1803.987,,597.029,9.59,,,37.746,0.5,64.83,0.511\n
AFG,Asia,Afghanistan,2020-02-27,1.0,0.0,,,,,0.026,0.0,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,8.33,38928341.0,54.422,18.6,2.581,1.337,1803.987,,597.029,9.59,,,37.746,0.5,64.83,0.511\n
AFG,Asia,Afghanistan,2020-02-28,1.0,0.0,,,,,0.026,0.0,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,8.33,38928341.0,54.422,18.6,2.581,1.337,1803.987,,597.029,9.59,,,37.746,0.5,64.83,0.511\n
AFG,Asia,Afghanistan,2020-02-29,1.0,0.0,0.143,,,0.0,0.026,0.0,0.004,,,0.0,,,,,,,,,,,,,,,,,,,,,,,,,,,,8.33,38928341.0,54.422,18.6,2.581,1.337,1803.987,,597.029,9.59,,,37.746,0.5,64.83,0.511\n
AFG,Asia,Afghanistan,2020-03-01,1.0,0.0,0.143,,,0.0,0.026,0.0,0.004,,,0.0,,,,,,,,,,,,,,,,,,,,,,,,,,,,27.78,38928341.0,54.422,18.6,2.581,1.337,1803.987,,597.029,9.59,,,37.746,0.5,64.83,0.511\n
AFG,Asia,Afghanistan,2020-03-02,1.0,0.0,0.0,,,0.0,0.026,0.0,0.0,,,0.0,,,,,,,,,,,,,,,,,,,,,,,,,,,,27.78,38928341.0,54.422,18.6,2.581,1.337,1803.987,,597.029,9.59,,,37.746,0.5,64.83,0.511\n
AFG,Asia,Afghanistan,2020-03-03,2.0,1.0,0.143,,,0.0,0.051,0.026,0.004,,,0.0,,,,,,,,,,,,,,,,,,,,,,,,,,,,27.78,38928341.0,54.422,18.6,2.581,1.337,1803.987,,597.029,9.59,,,37.746,0.5,64.83,0.511\n
AFG,Asia,Afghanistan,2020-03-04,4.0,2.0,0.429,,,0.0,0.103,0.051,0.011,,,0.0,,,,,,,,,,,,,,,,,,,,,,,,,,,,27.78,38928341.0,54.422,18.6,2.581,1.337,1803.987,,597.029,9.59,,,37.746,0.5,64.83,0.511\n
AFG,Asia,Afghanistan,2020-03-05,4.0,0.0,0.429,,,0.0,0.103,0.0,0.011,,,0.0,,,,,,,,,,,,,,,,,,,,,,,,,,,,27.78,38928341.0,54.422,18.6,2.581,1.337,1803.987,,597.029,9.59,,,37.746,0.5,64.83,0.511\n
AFG,Asia,Afghanistan,2020-03-06,4.0,0.0,0.429,,,0.0,0.103,0.0,0.011,,,0.0,,,,,,,,,,,,,,,,,,,,,,,,,,,,27.78,38928341.0,54.422,18.6,2.581,1.337,1803.987,,597.029,9.59,,,37.746,0.5,64.83,0.511`
	where string = "(continent='Asia' AND (date>'2020-04-14' AND date < '2020-04-20') OR (continent='Africa' AND '2020-04-14' != date))"
)

type splitData struct {
	Left, Right string
}

func TestSplitReverse(t *testing.T) {
	left, right := where, ""
	want := []splitData{
		{Left: "(continent='Asia' AND (date>'2020-04-14' AND date < '2020-04-20') OR (continent='Africa' AND '2020-04-14' != date)", Right: ")"},
		{Left: "(continent='Asia' AND (date>'2020-04-14' AND date < '2020-04-20') OR (continent='Africa' AND '2020-04-14' != date", Right: ")"},
		{Left: "(continent='Asia' AND (date>'2020-04-14' AND date < '2020-04-20') OR (continent='Africa' AND '2020-04-14' != ", Right: "date"},
		{Left: "(continent='Asia' AND (date>'2020-04-14' AND date < '2020-04-20') OR (continent='Africa' AND '2020-04-14' ", Right: "!="},
		{Left: "(continent='Asia' AND (date>'2020-04-14' AND date < '2020-04-20') OR (continent='Africa' AND ", Right: "'2020-04-14'"},
		{Left: "(continent='Asia' AND (date>'2020-04-14' AND date < '2020-04-20') OR (continent='Africa' ", Right: "AND"},
		{Left: "(continent='Asia' AND (date>'2020-04-14' AND date < '2020-04-20') OR (continent=", Right: "'Africa'"},
		{Left: "(continent='Asia' AND (date>'2020-04-14' AND date < '2020-04-20') OR (continent", Right: "="},
		{Left: "(continent='Asia' AND (date>'2020-04-14' AND date < '2020-04-20') OR (", Right: "continent"},
		{Left: "(continent='Asia' AND (date>'2020-04-14' AND date < '2020-04-20') OR ", Right: "("},
		{Left: "(continent='Asia' AND (date>'2020-04-14' AND date < '2020-04-20') ", Right: "OR"},
		{Left: "(continent='Asia' AND (date>'2020-04-14' AND date < '2020-04-20'", Right: ")"},
		{Left: "(continent='Asia' AND (date>'2020-04-14' AND date < ", Right: "'2020-04-20'"},
		{Left: "(continent='Asia' AND (date>'2020-04-14' AND date ", Right: "<"},
		{Left: "(continent='Asia' AND (date>'2020-04-14' AND ", Right: "date"},
		{Left: "(continent='Asia' AND (date>'2020-04-14' ", Right: "AND"},
		{Left: "(continent='Asia' AND (date>", Right: "'2020-04-14'"},
		{Left: "(continent='Asia' AND (date", Right: ">"},
		{Left: "(continent='Asia' AND (", Right: "date"},
		{Left: "(continent='Asia' AND ", Right: "("},
		{Left: "(continent='Asia' ", Right: "AND"},
		{Left: "(continent=", Right: "'Asia'"},
		{Left: "(continent", Right: "="},
		{Left: "(", Right: "continent"},
		{Left: "", Right: "("},
	}
	for _, val := range want {
		left, right = csv.SplitReverse(left)
		got := splitData{Left: left, Right: right}
		assert.Equal(t, val, got, "they should be equal")
	}
}
func TestGetLex(t *testing.T) {
	want := []string{"(", "continent", "=", "'Asia'", "AND", "(", "date", ">", "'2020-04-14'", "AND", "date", "<", "'2020-04-20'", ")", "OR", "(", "continent", "=", "'Africa'", "AND", "'2020-04-14'", "!=", "date", ")", ")"}
	got := csv.GetLex(where)
	assert.Equal(t, want, got, "they should be equal")
}
func TestInfixToPostfix(t *testing.T) {
	want := []string{"CONTINENT", "'ASIA'", "=", "DATE", "'2020-04-14'", ">", "DATE", "'2020-04-20'", "<", "AND", "CONTINENT", "'AFRICA'", "=", "'2020-04-14'", "DATE", "!=", "AND", "OR", "AND"}
	got := csv.InfixToPostfix(csv.GetLex(where))
	assert.Equal(t, want, got, "func \"TestGetItem\": they should be equal")
}

func TestGetFields(t *testing.T) {
	want := []string{
		"iso_code", "continent", "location", "date", "total_cases", "new_cases", "new_cases_smoothed",
		"total_deaths", "new_deaths", "new_deaths_smoothed", "total_cases_per_million", "new_cases_per_million",
		"new_cases_smoothed_per_million", "total_deaths_per_million", "new_deaths_per_million", "new_deaths_smoothed_per_million",
		"reproduction_rate", "icu_patients", "icu_patients_per_million", "hosp_patients", "hosp_patients_per_million",
		"weekly_icu_admissions", "weekly_icu_admissions_per_million", "weekly_hosp_admissions", "weekly_hosp_admissions_per_million",
		"new_tests", "total_tests", "total_tests_per_thousand", "new_tests_per_thousand", "new_tests_smoothed",
		"new_tests_smoothed_per_thousand", "positive_rate", "tests_per_case", "tests_units", "total_vaccinations",
		"people_vaccinated", "people_fully_vaccinated", "new_vaccinations", "new_vaccinations_smoothed",
		"total_vaccinations_per_hundred", "people_vaccinated_per_hundred", "people_fully_vaccinated_per_hundred",
		"new_vaccinations_smoothed_per_million", "stringency_index", "population", "population_density", "median_age",
		"aged_65_older", "aged_70_older", "gdp_per_capita", "extreme_poverty", "cardiovasc_death_rate", "diabetes_prevalence",
		"female_smokers", "male_smokers", "handwashing_facilities", "hospital_beds_per_thousand", "life_expectancy", "human_development_index",
	}
	for i := range want {
		want[i] = strings.ToUpper(want[i])
	}
	got := csv.GetFields(firstRow, ",")
	assert.Equal(t, want, got, "func \"TestGetItem\": they should be equal")
}

func TestReplaceFieldsToValues(t *testing.T) {
	lex := []string{"(", "CONTINENT", "=", "'Asia'", "AND", "(", "DATE", ">", "'2020-04-14'", "AND", "DATE", "<", "'2020-04-20'", ")", "OR", "(", "CONTINENT", "=", "'Africa'", "AND", "'2020-04-14'", "!=", "DATE", ")", ")"}
	want := [][]string{
		{"(", "'Asia'", "=", "'Asia'", "AND", "(", "'2020-02-24'", ">", "'2020-04-14'", "AND", "'2020-02-24'", "<", "'2020-04-20'", ")", "OR", "(", "'Asia'", "=", "'Africa'", "AND", "'2020-04-14'", "!=", "'2020-02-24'", ")", ")"},
		{"(", "'Asia'", "=", "'Asia'", "AND", "(", "'2020-02-25'", ">", "'2020-04-14'", "AND", "'2020-02-25'", "<", "'2020-04-20'", ")", "OR", "(", "'Asia'", "=", "'Africa'", "AND", "'2020-04-14'", "!=", "'2020-02-25'", ")", ")"},
		{"(", "'Asia'", "=", "'Asia'", "AND", "(", "'2020-02-26'", ">", "'2020-04-14'", "AND", "'2020-02-26'", "<", "'2020-04-20'", ")", "OR", "(", "'Asia'", "=", "'Africa'", "AND", "'2020-04-14'", "!=", "'2020-02-26'", ")", ")"},
		{"(", "'Asia'", "=", "'Asia'", "AND", "(", "'2020-02-27'", ">", "'2020-04-14'", "AND", "'2020-02-27'", "<", "'2020-04-20'", ")", "OR", "(", "'Asia'", "=", "'Africa'", "AND", "'2020-04-14'", "!=", "'2020-02-27'", ")", ")"},
		{"(", "'Asia'", "=", "'Asia'", "AND", "(", "'2020-02-28'", ">", "'2020-04-14'", "AND", "'2020-02-28'", "<", "'2020-04-20'", ")", "OR", "(", "'Asia'", "=", "'Africa'", "AND", "'2020-04-14'", "!=", "'2020-02-28'", ")", ")"},
		{"(", "'Asia'", "=", "'Asia'", "AND", "(", "'2020-02-29'", ">", "'2020-04-14'", "AND", "'2020-02-29'", "<", "'2020-04-20'", ")", "OR", "(", "'Asia'", "=", "'Africa'", "AND", "'2020-04-14'", "!=", "'2020-02-29'", ")", ")"},
		{"(", "'Asia'", "=", "'Asia'", "AND", "(", "'2020-03-01'", ">", "'2020-04-14'", "AND", "'2020-03-01'", "<", "'2020-04-20'", ")", "OR", "(", "'Asia'", "=", "'Africa'", "AND", "'2020-04-14'", "!=", "'2020-03-01'", ")", ")"},
		{"(", "'Asia'", "=", "'Asia'", "AND", "(", "'2020-03-02'", ">", "'2020-04-14'", "AND", "'2020-03-02'", "<", "'2020-04-20'", ")", "OR", "(", "'Asia'", "=", "'Africa'", "AND", "'2020-04-14'", "!=", "'2020-03-02'", ")", ")"},
		{"(", "'Asia'", "=", "'Asia'", "AND", "(", "'2020-03-03'", ">", "'2020-04-14'", "AND", "'2020-03-03'", "<", "'2020-04-20'", ")", "OR", "(", "'Asia'", "=", "'Africa'", "AND", "'2020-04-14'", "!=", "'2020-03-03'", ")", ")"},
		{"(", "'Asia'", "=", "'Asia'", "AND", "(", "'2020-03-04'", ">", "'2020-04-14'", "AND", "'2020-03-04'", "<", "'2020-04-20'", ")", "OR", "(", "'Asia'", "=", "'Africa'", "AND", "'2020-04-14'", "!=", "'2020-03-04'", ")", ")"},
		{"(", "'Asia'", "=", "'Asia'", "AND", "(", "'2020-03-05'", ">", "'2020-04-14'", "AND", "'2020-03-05'", "<", "'2020-04-20'", ")", "OR", "(", "'Asia'", "=", "'Africa'", "AND", "'2020-04-14'", "!=", "'2020-03-05'", ")", ")"},
		{"(", "'Asia'", "=", "'Asia'", "AND", "(", "'2020-03-06'", ">", "'2020-04-14'", "AND", "'2020-03-06'", "<", "'2020-04-20'", ")", "OR", "(", "'Asia'", "=", "'Africa'", "AND", "'2020-04-14'", "!=", "'2020-03-06'", ")", ")"},
	}

	head := csv.Head{}
	head.Fields = csv.GetFields(firstRow, ",")
	row := head.NewRow()
	rowsStr := strings.Split(data, "\n")
	for i, val := range rowsStr {
		l := make([]string, len(lex))
		copy(l, lex)
		row.Values = strings.Split(val, ",")
		csv.ReplaceFieldsToValues(l, row)
		assert.Equal(t, want[i], l, "they should be equal")
	}
}

func TestGetBoolResult(t *testing.T) {
	input := [][]string{
		{"(", "'Asia'", "=", "'Asia'", "AND", "(", "'2020-02-24'", ">", "'2020-04-14'", "AND", "'2020-02-24'", "<", "'2020-04-20'", ")", "OR", "(", "'Asia'", "=", "'Africa'", "AND", "'2020-04-14'", "!=", "'2020-02-24'", ")", ")"},
		{"(", "'Asia'", "=", "'Asia'", "AND", "(", "'2020-02-25'", ">", "'2020-04-14'", "AND", "'2020-02-25'", "<", "'2020-04-20'", ")", "OR", "(", "'Africa'", "=", "'Africa'", "AND", "'2020-04-14'", "!=", "'2020-02-25'", ")", ")"},
		{"(", "'Asia'", "=", "'Asia'", "AND", "(", "'2020-05-26'", ">", "'2020-04-14'", "AND", "'2020-02-26'", "<", "'2020-04-20'", ")", "OR", "(", "'Asia'", "=", "'Africa'", "AND", "'2020-04-14'", "!=", "'2020-02-26'", ")", ")"},
		{"(", "'Asia'", "=", "'Asia'", "AND", "(", "'2020-02-27'", ">", "'2020-04-14'", "AND", "'2020-02-27'", "<", "'2020-04-20'", ")", "OR", "(", "'Asia'", "=", "'Africa'", "AND", "'2020-04-14'", "!=", "'2020-02-27'", ")", ")"},
	}
	type result struct {
		res bool
		err error
	}
	want := []result{
		{res: false, err: nil},
		{res: true, err: nil},
		{res: true, err: nil},
		{res: false, err: nil},
	}
	for i, val := range input {
		got, err := csv.GetBoolResult(csv.InfixToPostfix(val))
		assert.Equal(t, want[i], result{res: got, err: err})
	}
}

func TestIsMatch(t *testing.T) {
	want := []bool{false, true, false, false, false, false, false, false, true, false, false, false}
	head := csv.Head{}
	head.Fields = csv.GetFields(firstRow, ",")
	row := head.NewRow()
	rowsStr := strings.Split(data, "\n")

	for i, val := range rowsStr {
		row.Values = strings.Split(val, ",")
		got := row.IsMatch("continent='Asia' AND (date='2020-02-25' OR date='2020-03-03')")
		assert.Equal(t, want[i], got, "round # %s", i)
	}
}
