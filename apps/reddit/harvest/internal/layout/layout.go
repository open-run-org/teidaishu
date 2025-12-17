package layout

import (
	"path/filepath"
	"time"
)

func Base(root, sub string) string {
	return filepath.Join(root, "r_"+sub)
}

func SplitUTC(unix int64) (string, string, string) {
	t := time.Unix(unix, 0).UTC()
	return t.Format("2006"), t.Format("0102"), t.Format("150405")
}

func ThreadRel(createdUnix int64, id string) string {
	y, md, hms := SplitUTC(createdUnix)
	return filepath.Join(y, md, hms+"_"+id)
}
