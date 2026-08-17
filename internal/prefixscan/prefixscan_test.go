package prefixscan_test
import ("testing"; "github.com/LYH2263/go-prefix-scan/internal/prefixscan")
func TestSeekInclusive(t *testing.T) {
	it := prefixscan.New([]string{"ab", "abc", "b"})
	it.Seek("ab")
	if it.Key() != "ab" { t.Fatalf("key=%q", it.Key()) }
}
