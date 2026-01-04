package internal

// Sealer is an exported token type placed in the internal package so external
// modules cannot import it. Requiring this type as a parameter to an
// interface method prevents other modules from implementing that interface.
// Packages inside the same module may import this package and therefore can
// implement the sealed interface if needed.
type Sealer struct{}
