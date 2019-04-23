package service

type Service interface {
	CreateService() Service

	StartProvider(providerURL string) error
}
