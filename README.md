# Weave

Weave provides the ability to turn data stored in arbitrary structs into exportable formats like CSV or JSON.
It supports field selection, named/unnamed structs, and embeds.

# Usage

```go
type someEmbed struc {
	Fld int
}

type someData struct {
	someEmbed
	A int
}
	data := someData{someEmbed: someEmbed{Fld: 5}, A: 10}

	output := ToCSV(data, []string{"A"})

	fmt.Println(output)
```
