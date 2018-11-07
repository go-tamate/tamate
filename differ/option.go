package differ

type Option func(*Differ) error

func IgnoreColumn(name string) Option {
	return func(d *Differ) error {
		return d.setIgnoreColumnName(name)
	}
}
