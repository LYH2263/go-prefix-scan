package prefixscan
type Iter struct { keys []string; i int }
func New(keys []string) *Iter { return &Iter{keys: keys} }
func (it *Iter) Seek(prefix string) {
	it.i = 0
	for it.i < len(it.keys) && it.keys[it.i] <= prefix { it.i++ }
}
func (it *Iter) Key() string { if it.i >= len(it.keys) { return "" }; return it.keys[it.i] }
