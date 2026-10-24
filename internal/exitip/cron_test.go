package exitip

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCronBasics(t *testing.T) {
	// wildcard: every minute
	s, err := ParseCron("* * * * *")
	require.NoError(t, err)
	assert.True(t, s.Match(time.Date(2026, 10, 24, 13, 7, 0, 0, time.UTC)))

	// explicit minute/hour
	s, err = ParseCron("30 4 * * *")
	require.NoError(t, err)
	assert.True(t, s.Match(time.Date(2026, 10, 24, 4, 30, 0, 0, time.UTC)))
	assert.False(t, s.Match(time.Date(2026, 10, 24, 4, 31, 0, 0, time.UTC)))
	assert.False(t, s.Match(time.Date(2026, 10, 24, 5, 30, 0, 0, time.UTC)))

	// day-of-week: Sunday as 0 and as 7
	for _, spec := range []string{"0 0 * * 0", "0 0 * * 7"} {
		s, err = ParseCron(spec)
		require.NoError(t, err, spec)
		sun := time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC) // a Sunday
		assert.True(t, s.Match(sun), spec)
		assert.False(t, s.Match(sun.Add(24*time.Hour)), spec)
	}

	// lists and ranges
	s, err = ParseCron("0 0 1,15 * *")
	require.NoError(t, err)
	assert.True(t, s.Match(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)))
	assert.True(t, s.Match(time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)))
	assert.False(t, s.Match(time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)))

	s, err = ParseCron("*/15 * * * *")
	require.NoError(t, err)
	assert.True(t, s.Match(time.Date(2026, 11, 2, 3, 45, 0, 0, time.UTC)))
	assert.False(t, s.Match(time.Date(2026, 11, 2, 3, 44, 0, 0, time.UTC)))

	s, err = ParseCron("0 9-17 * * 1-5")
	require.NoError(t, err)
	assert.True(t, s.Match(time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)))  // Friday noon
	assert.False(t, s.Match(time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC))) // Saturday noon
	assert.False(t, s.Match(time.Date(2026, 10, 23, 18, 0, 0, 0, time.UTC))) // Friday 6pm
}

// TestParseCronDomDowOr verifies Vixie semantics: when both day-of-month and
// day-of-week are restricted, a time matches when EITHER matches.
func TestParseCronDomDowOr(t *testing.T) {
	s, err := ParseCron("0 0 1 * 1") // 1st of month OR any Monday
	require.NoError(t, err)
	assert.True(t, s.Match(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)))  // the 1st (a Sunday)
	assert.True(t, s.Match(time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC))) // a Monday, not the 1st
	assert.False(t, s.Match(time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)))

	// Only dom restricted: dow field "*" does not loosen the dom.
	s, err = ParseCron("0 0 1 * *")
	require.NoError(t, err)
	assert.False(t, s.Match(time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)))
}

func TestParseCronErrors(t *testing.T) {
	for _, spec := range []string{
		"",
		"* * * *",       // 4 fields
		"* * * * * *",   // 6 fields
		"60 * * * *",    // minute out of range
		"* 24 * * *",    // hour out of range
		"* * 0 * *",     // dom below 1
		"* * * 13 *",    // month out of range
		"* * * * 8",     // dow out of range
		"a * * * *",     // non-numeric
		"*/0 * * * *",   // zero step
		"1-2-3 * * * *", // malformed range
		"5-1 * * * *",   // inverted range
	} {
		_, err := ParseCron(spec)
		assert.Error(t, err, spec)
	}
}

func TestCronNext(t *testing.T) {
	s, err := ParseCron("*/15 * * * *")
	require.NoError(t, err)
	from := time.Date(2026, 10, 24, 12, 7, 30, 0, time.UTC)
	next, err := s.Next(from)
	require.NoError(t, err)
	assert.Equal(t, 15, next.Minute())
	assert.Equal(t, 12, next.Hour())
}

// TestCronNextImpossible verifies the scan-horizon error on an unsatisfiable
// expression (Feb 31 does not exist).
func TestCronNextImpossible(t *testing.T) {
	s, err := ParseCron("0 0 31 2 *")
	require.NoError(t, err) // syntactically fine
	_, err = s.Next(time.Now())
	require.Error(t, err)
}
