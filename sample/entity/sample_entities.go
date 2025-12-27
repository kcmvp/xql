package entity

import "time"

type Dummy struct {
	TestingName string
}

// BaseEntity defines common fields for database entities.
//
// NOTE: We don't model relationships in structs (no slices/pointers for relations),
// and we don't rely on DB foreign keys. Joins are built purely via fields.
type BaseEntity struct {
	ID        int64 `xql:"pk"`
	CreatedAt time.Time
	UpdatedAt time.Time
	CreatedBy string
	UpdatedBy string
}

// Account represents a user account in the system.
//
// Joins:
//   - 1:N with Order via Order.AccountID
//   - N:N with Role via AccountRole join-table
type Account struct {
	BaseEntity
	Dummy    Dummy
	Email    string `xql:"unique;index"`
	Nickname string `xql:"name:nick_name;type:varchar(100);unique;not null;default:'anonymous'"`
	Category int64  `xql:"type:integer;default:0"`
	Balance  float64
}

func (a Account) Table() string { return "accounts" }

// Profile represents a 1:1 extension of Account.
//
// Joins:
//   - 1:1 with Account via Profile.AccountID
type Profile struct {
	BaseEntity
	Dummy     Dummy
	AccountID int64
	Bio       string
	Birthday  time.Time
}

func (p Profile) Table() string { return "profiles" }

// Order represents a customer order.
//
// Joins:
//   - N:1 with Account via Order.AccountID
//   - 1:N with OrderItem via OrderItem.OrderID
type Order struct {
	BaseEntity
	Dummy         Dummy
	AccountID     int64
	Amount        float64
	internalNotes string
}

func (o Order) Table() string { return "orders" }

// OrderItem represents a line item of an Order (1:N).
//
// Joins:
//   - N:1 with Order via OrderItem.OrderID
//   - N:1 with Product via OrderItem.ProductID
type OrderItem struct {
	BaseEntity
	Dummy     Dummy
	OrderID   int64
	ProductID int64
	Quantity  int64
	UnitPrice float64 `xql:"name:unit_price; type:decimal(10,2)"`
}

func (oi OrderItem) Table() string { return "order_items" }

// Product represents a product.
//
// Joins:
//   - N:N with Order via OrderItem
type Product struct {
	BaseEntity
	Dummy Dummy
	SKU   string `xql:"unique;index"`
	Name  string
	Price float64
}

func (p Product) Table() string { return "products" }

// Role represents an authorization role.
//
// Joins:
//   - N:N with Account via AccountRole
type Role struct {
	BaseEntity
	Dummy Dummy
	Key   string `xql:"unique;index"`
	Name  string
}

func (r Role) Table() string { return "roles" }

// AccountRole is a join table for Account <-> Role.
//
// Joins:
//   - N:1 with Account via AccountRole.AccountID
//   - N:1 with Role via AccountRole.RoleID
type AccountRole struct {
	BaseEntity
	Dummy     Dummy
	AccountID int64
	RoleID    int64
}

func (ar AccountRole) Table() string { return "account_roles" }
