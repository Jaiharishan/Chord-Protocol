use "collections"
use "promises"

primitive ChordConfig
  fun node_bits(): USize => 6  // 2^6 = 64 possible positions
  fun max_nodes(): USize => 1 << node_bits()

class val NodeInfo
  let id: USize
  let actor_ref: Node tag
  
  new val create(id': USize, ref': Node tag) =>
    id = id'
    actor_ref = ref'

actor Node
  let _id: USize
  let _env: Env
  var _successor: (NodeInfo | None)
  var _predecessor: (NodeInfo | None)
  let _finger_table: Array[NodeInfo]
  
  new create(env: Env, id': USize) =>
    _id = id'
    _env = env
    _successor = None
    _predecessor = None
    _finger_table = Array[NodeInfo](0)
    

  // Initalize the Node and the Finger Table
  be initialize(nodes: Array[NodeInfo] val) =>
    try
      // Set immediate successor
      for node in nodes.values() do
        if node.id > _id then
          _successor = node
          break
        end
      end

      // Set the wrap around successor
      if _successor is None then
        _successor = nodes(0)?  
      end

      // Creating Table with embedded binary search for closest node
      for i in Range[USize](0, ChordConfig.node_bits()) do
        let target_id = (_id + (1 << i)) % ChordConfig.max_nodes()
        
        // Inline binary search to find closest node to target_id
        var low: USize = 0
        var high: USize = nodes.size() - 1
        var closest_index: USize = 69  // Sentinel for not found

        while low <= high do
            let mid = (low + high) / 2
            if nodes(mid)?.id == target_id then
                closest_index = mid
                break
            elseif nodes(mid)?.id < target_id then                        
                low = mid + 1
                closest_index = mid  // Closest found so far
            else
                high = mid - 1
            end
        end 

        // _env.out.print("For Node " + _id.string() + " Finger Table " + i.string() + ":" + nodes(closest_index)?.id.string())

        // if the nodes(index) equals the current element then push the successor
        // if nodes(closest_index)?.id == _id then
        //     _finger_table.push(nodes((_id + 1) % ChordConfig.max_nodes())?)  // Use partial assignment with `?`
        //     continue
        // end

        // Assign the closest node found or wrap around to the first node
        if closest_index != 69 then
                // _env.out.print("For Node" + _id.string() + "Found Node: " + nodes(closest_index)?.id.string())
                _finger_table.push(nodes(closest_index)?)  // Use partial assignment with `?`
        else
            // _env.out.print("For Node" + _id.string() + "Wrap around to: " + nodes(0)?.id.string())
            _finger_table.push(nodes(0)?)  // Wrap around if no closer node is found
        end 
      end 

    // Output finger table
      for (i, node) in _finger_table.pairs() do
        _env.out.print("For Node " + _id.string() + "  |  " + i.string() + ": " + node.id.string())
      end

      _env.out.print("Finger Table Created for Node" + _id.string())

    else
      _env.out.print("Failed to initialize Node " + _id.string())
    end

// Lookup algorithm to find the node that contains the key
// This is an recursive algorithm that searches the network for the node that contains the key
  be lookup(key: USize, p: Promise[(NodeInfo, USize)], hops: USize = 0, nodes: Array[NodeInfo] val) =>
    try 
        _env.out.print("Looking up key " + key.string() + " in Node " + _id.string())
        // Step 1: Check if the key is equal to the current node's ID
        if key == _id then
            p((NodeInfo(_id, this), hops))

        // Check if the key lies in the range of [predecessor, current] then also the node contains the key

        // Check if the key lies in the finger table
        else
            // Step 2: Linear search in the finger table to find the range
            var target_node: (NodeInfo | None) = None

            if _finger_table.size() > 0 then
                _env.out.print("Finger table NOT empty")
            end

            // Find target node within finger table ranges
            for i in Range(0, _finger_table.size() - 1) do
                var lower_bound = _finger_table(i)?
                var upper_bound = _finger_table(i + 1)?

                if lower_bound.id > upper_bound.id then
                    var tmp = lower_bound
                    lower_bound = upper_bound
                    upper_bound = tmp
                end

                if (key > lower_bound.id) and (key <= upper_bound.id) then
                    _env.out.print("Found target node: " + upper_bound.id.string() + " From Node " + _id.string())
                    target_node = upper_bound
                    break
                end
            end

            if target_node is None then
                target_node = _finger_table(_finger_table.size() - 1)?
            end

            // Recursive forwarding
            match target_node
            | let node: NodeInfo =>
                if node.id == _id then
                    p((NodeInfo(_id, this), hops))
                else
                    node.actor_ref.lookup(key, p, hops + 1, nodes)
                end
            else
                p((NodeInfo(_id, this), hops))
            end
        end

    else
        _env.out.print("Failed to lookup key " + key.string())
    end


  // Returns true if id is between start and finish
  fun _in_range(id: USize, start: USize, finish: USize): Bool =>
    if start < finish then
      (id > start) and (id <= finish)
    else
      (id > start) or (id <= finish)
    end



// Main Actor
actor Main
  new create(env: Env) =>
    try
      // Get number of nodes from command line or default to 12
      let num_nodes = 
        if env.args.size() > 1 then
          env.args(1)?.usize()?
        else
          12
        end
      
      // Get key to lookup from command line or default to 4
      let key = 
        if env.args.size() > 2 then
          env.args(2)?.usize()?
        else
          4
        end
      
      env.out.print("Creating Chord network with " + num_nodes.string() + " nodes")
      env.out.print("Will lookup key: " + key.string())
      env.out.print("")
      
      // Create actors
      let actors = Array[Node](num_nodes)
      
      // Create nodes and build nodes array within recover block
      // Nodes are equally distributed across the circular network
      let nodes = recover val
        let arr = Array[NodeInfo](num_nodes)
        for i in Range[USize](0, num_nodes) do
          let id = (i * ChordConfig.max_nodes()) / num_nodes
          let node = Node(env, id)
          actors.push(node)
          arr.push(NodeInfo(id, node))
        //   env.out.print("Node " + id.string() + " created")
        end
        arr
      end

      for node in nodes.values() do
        env.out.print("Node " + node.id.string() + " created")
      end
      
      // Initialize nodes
      for node in actors.values() do
        node.initialize(nodes)
      end

      // Perform lookup
      let p = Promise[(NodeInfo, USize)]

      if actors.size() > 0 then
        actors(0)?.lookup(key, p, 0, nodes)
        
        let notify = object iso
          let _env: Env = env
          fun ref apply(result: (NodeInfo, USize)) =>
            let info = result._1
            let hops = result._2
            _env.out.print("\nLookup result:")
            _env.out.print("Key " + key.string() + " is handled by node " + info.id.string())
            _env.out.print("Total hops taken: " + hops.string())
          fun ref dispose() => None
      end
        
        p.next[None](consume notify)
      end
    else
      env.out.print("Usage: chord [num_nodes] [key]")
      env.out.print("  num_nodes: number of nodes in network (default: 12)")
      env.out.print("  key: key to lookup (default: 4)")
    end
