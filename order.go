package gobptree

import "fmt"

func checkOrder(order int) error {
	if order >= 2 && order&(order-1) == 0 {
		return nil
	}
	return fmt.Errorf("cannot create tree when order is less than 2 or not a power of 2: %d", order)
}
