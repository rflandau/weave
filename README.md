# Weave

Weave provides the ability to turn data stored in arbitrary structs into exportable formats like CSV or JSON.
It supports field selection, named/unnamed structs, and embeds.

# Usage

Basic usage is via the output modules (`To*`). Simply pass your data to an output module along with the fully qualified (more on this below) names of the columns you want outputted. The data must be an *array of the same struct*.

Ex: `out := ToCSV(data, []string{"fieldname", "structname.anotherinnerstruct.fieldname"})`

Call `StructField()` on your struct to see the full, qualified names of every field at every depth.

## Example

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

## Dot Qualification

Column names are dot qualified and follow Go's rules for struct nesting and promotion.

To repeat: call `StructField()` on your struct to see the full, qualified names of every field at every depth.

### Examples

#### Basic

```go
type A struct {
	a int
	b int
	C int
}
```

Can be accessed directly ("a", "b", "C").

#### Embedding

```go
type mbd struct {
	X string
	z string
}

type A struct {
	a int
	b int
	C int
	mbd
}
```

Embedded field are accessed as "X" and "z".

#### Structs Within Structs

```go
type deep struct {
	F float 64
}

type shallow struct {
	D deep
	X string
	z string
}

type A struct {
	a int
	b int
	C int
	i inner
}
```

"i.D.F", "i.z"

