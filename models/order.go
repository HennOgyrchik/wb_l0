package models

type Order struct {
	OrderUid          string
	TrackNumber       string
	Entry             string
	Delivery          Delivery
	Payment           Payment
	Items             []Item
	Locale            string
	InternalSignature string
	CustomerId        string
	DeliveryService   string
	Shardkey          string
	SmId              int
	DateCreated       string //может дата?
	OofShard          string
}

type Delivery struct {
	Name,
	Phone,
	Zip,
	City,
	Address,
	Region,
	Email string
}

type Payment struct {
	Transaction,
	RequestId,
	Currency,
	Provider string
	Amount,
	PaymentDt int
	Bank string
	DeliveryCost,
	GoodsTotal,
	CustomFee int
}

type Item struct {
	ChrtId      int
	TrackNumber string
	Price       int
	Rid         string
	Name        string
	Sale        int
	Size        string
	TotalPrice  int
	NmId        int
	Brand       string
	Status      int
}
