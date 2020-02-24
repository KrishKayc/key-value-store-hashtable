package db

//uses hashtable to store the data in file
type hashtableStore struct {
	sm      storageManager
	nodeCap int64 //determines the max capacity a single node will occupy
}

func newHashtableStore(filepath string) *hashtableStore {

	return &hashtableStore{sm: newFileStorageManager(filepath), nodeCap: capsize}
}

func (ht *hashtableStore) addNode(n *Node) {

	pos := ht.hash(n.Key)         //get the hash of the key
	pos = ht.handleCollision(pos) //change the position if there is a collision
	n.Pos = pos
	ht.sm.write(encode(n), n.Pos)

}

func (ht *hashtableStore) getNode(key string) (*Node, error) {

	pos := ht.hash(key)
	return ht.getNodeByPos(key, pos)
}

//Looks up for the key in the position.
//If the key isn't there, get colliding node and look up in the next linked nodes recursively
func (ht *hashtableStore) getNodeByPos(key string, pos int64) (*Node, error) {

	node := ht.getCollidingNode(pos)

	if node == nil {
		return nil, &keyNotFoundError{}
	}
	if node.Key == key {
		return node, nil
	}

	if node.NextPos > 0 {
		buff, _ := ht.sm.read(ht.nodeCap, node.NextPos)
		n := decode(buff)

		if n.Key == key {
			return n, nil
		}
		return ht.getNodeByPos(key, n.NextPos)
	}

	return nil, &keyNotFoundError{}

}

func (ht *hashtableStore) deleteNode(key string) {

	n, _ := ht.getNode(key)
	ht.deleteNodeByPos(key, n.Pos)

}
func (ht *hashtableStore) deleteNodeByPos(key string, pos int64) {

	empty := make([]byte, ht.nodeCap)
	prevPos := pos - ht.nodeCap
	nextPos := pos + ht.nodeCap

	ht.sm.write(empty, pos)

	//If there are linked nodes due to collision, remap them after deleting this
	prevBuf, _ := ht.sm.read(ht.nodeCap, prevPos)
	nextBuf, _ := ht.sm.read(ht.nodeCap, nextPos)

	prevNode := decode(prevBuf)
	nextNode := decode(nextBuf)

	if prevNode.Key != "" && nextNode.Key != "" {
		prevNode.NextPos = nextNode.Pos

	}

	ht.sm.write(encode(prevNode), prevNode.Pos)
}
func (ht *hashtableStore) isfull() bool {
	return ht.sm.isfull()
}

func (ht *hashtableStore) close() {
	ht.sm.close()
}

func (ht *hashtableStore) hash(val string) int64 {
	return basicHash(val)
}

func basicHash(val string) int64 {
	var sum int
	for i, c := range val {
		sum = sum + (i * int(c))
	}

	return int64(sum)
}

//Checks for collision in the passed location and handes it
func (ht *hashtableStore) handleCollision(pos int64) int64 {

	collNode := ht.getCollidingNode(pos)

	if collNode == nil {
		return pos
	}
	//if we get a colliding node, add the reference link to new node
	nextPos := collNode.Pos + ht.nodeCap
	collNode.NextPos = nextPos
	ht.sm.write(encode(collNode), collNode.Pos)
	return collNode.NextPos
}

func hasData(b []byte) (bool, int) {

	for i, val := range b {
		if val != 0 {
			return true, i
		}
	}
	return false, 0
}

//gets the colliding node, if any's have collide with already existing data
func (ht *hashtableStore) getCollidingNode(currentPos int64) *Node {
	buff, _ := ht.sm.read(ht.nodeCap, currentPos)
	hasData, i := hasData(buff)
	if !hasData {
		return nil
	}

	n := decode(buff)

	if n.Key == "" {
		var adjustedPos int64
		if i == 0 {
			adjustedPos = currentPos - 1
		} else {
			adjustedPos = currentPos + 1
		}
		return ht.getCollidingNode(adjustedPos)
	}

	return n
}
