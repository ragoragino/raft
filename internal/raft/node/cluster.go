package node

type ICluster interface {
	GetLeaderIP() string
}

type Cluster struct {
}
