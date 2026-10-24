// Minimal 5-field crontab schedule parsing (minute hour day-of-month month
// day-of-week). Supports "*", single values, ranges (a-b), lists (a,b,c),
// and steps ("*/n", "a-b/n"). Day-of-week 0 and 7 both mean Sunday. Fields
// obey standard Vixie cron semantics: when both day-of-month and
// day-of-week are restricted (not "*"), a time matches when EITHER matches.
package exitip

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// limits for each field.
var fieldLimits = [5]struct{ min, max int }{
	{0, 59}, // minute
	{0, 23}, // hour
	{1, 31}, // day of month
	{1, 12}, // month
	{0, 7},  // day of week (7 == Sunday, normalized to 0)
}

// Schedule is a parsed cron expression.
type Schedule struct {
	minutes uint64 // bit i set = minute i allowed
	hours   uint64
	doms    uint64
	months  uint64
	dows    uint64
	// domRestricted / dowRestricted record whether those fields were
	// anything other than "*" (Vixie OR semantics).
	domRestricted bool
	dowRestricted bool
}

// ParseCron parses spec ("m h dom mon dow") into a Schedule.
func ParseCron(spec string) (*Schedule, error) {
	fields := strings.Fields(spec)
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron: expected 5 fields (minute hour dom month dow), got %d", len(fields))
	}
	s := &Schedule{}
	bits := []*uint64{&s.minutes, &s.hours, &s.doms, &s.months, &s.dows}
	for i, f := range fields {
		lim := fieldLimits[i]
		set, all, err := parseField(f, lim.min, lim.max)
		if err != nil {
			return nil, fmt.Errorf("cron: field %d (%q): %w", i+1, f, err)
		}
		if i == 2 {
			s.domRestricted = !all
		}
		if i == 4 {
			s.dowRestricted = !all
		}
		if i == 4 {
			// normalize Sunday-7 into bit 0
			if set&(1<<7) != 0 {
				set |= 1
			}
			set &^= 1 << 7
		}
		*bits[i] = set
	}
	return s, nil
}

// parseField parses one field into a bitmask. Returns all=true when the
// field allows every value (bare "*").
func parseField(f string, min, max int) (set uint64, all bool, err error) {
	step := 1
	// split off "/step"
	if i := strings.IndexByte(f, '/'); i >= 0 {
		n, err0 := strconv.Atoi(f[i+1:])
		if err0 != nil || n < 1 {
			return 0, false, fmt.Errorf("bad step %q", f[i+1:])
		}
		step = n
		f = f[:i]
	}
	if f == "*" {
		for v := min; v <= max; v += step {
			set |= 1 << uint(v)
		}
		return set, step == 1, nil
	}
	for _, part := range strings.Split(f, ",") {
		lo, hi := part, part
		if i := strings.IndexByte(part, '-'); i >= 0 {
			lo, hi = part[:i], part[i+1:]
		}
		l, err0 := strconv.Atoi(lo)
		if err0 != nil {
			return 0, false, fmt.Errorf("bad value %q", part)
		}
		h, err0 := strconv.Atoi(hi)
		if err0 != nil {
			return 0, false, fmt.Errorf("bad value %q", part)
		}
		if l < min || h > max || l > h {
			return 0, false, fmt.Errorf("value %q out of range [%d,%d]", part, min, max)
		}
		for v := l; v <= h; v += step {
			set |= 1 << uint(v)
		}
	}
	return set, false, nil
}

// Match reports whether t satisfies the schedule.
func (s *Schedule) Match(t time.Time) bool {
	if s.minutes&(1<<uint(t.Minute())) == 0 {
		return false
	}
	if s.hours&(1<<uint(t.Hour())) == 0 {
		return false
	}
	if s.months&(1<<uint(int(t.Month()))) == 0 {
		return false
	}
	domOK := s.doms&(1<<uint(t.Day())) != 0
	dowOK := s.dows&(1<<uint(int(t.Weekday()))) != 0
	if s.domRestricted && s.dowRestricted {
		return domOK || dowOK
	}
	return domOK && dowOK
}

// Next returns the first time strictly after t that matches, or an error if
// no match exists within the scan horizon (5 years — enough for any real
// schedule; hitting it means an impossible expression like "0 0 31 2 *").
func (s *Schedule) Next(t time.Time) (time.Time, error) {
	// Start at the next whole minute strictly after t.
	cand := t.Truncate(time.Minute).Add(time.Minute)
	limit := cand.Add(5 * 365 * 24 * time.Hour)
	for cand.Before(limit) {
		if s.Match(cand) {
			return cand, nil
		}
		cand = cand.Add(time.Minute)
	}
	return time.Time{}, fmt.Errorf("cron: no match within 5 years (impossible schedule?)")
}
