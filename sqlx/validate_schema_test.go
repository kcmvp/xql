package sqlx

import (
	"testing"

	"github.com/kcmvp/xql/sample/entity"
	acctpkg "github.com/kcmvp/xql/sample/gen/field/account"
	orderpkg "github.com/kcmvp/xql/sample/gen/field/order"
	"github.com/stretchr/testify/require"
)

func TestValidateSchemaForEntity_Success(t *testing.T) {
	// use generated Account fields
	schema := Schema{acctpkg.ID, acctpkg.Email}
	err := validateSyntax[entity.Account](schema...)
	require.NoError(t, err)
}

func TestValidateSchemaForEntity_Mismatch(t *testing.T) {
	// mix an account field with an order field (different table)
	schema := Schema{acctpkg.ID, orderpkg.Amount}
	err := validateSyntax[entity.Account](schema...)
	require.Error(t, err)
	// ensure error mentions offending field qualified name
	require.Contains(t, err.Error(), orderpkg.Amount.QualifiedName())
}
