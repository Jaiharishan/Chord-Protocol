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
    _finger_table = Array[NodeInfo](ChordConfig.node_bits())
    
  be initialize(nodes: Array[NodeInfo] val) =>
    try
      // Set immediate successor
      for node in nodes.values() do
        if node.id > _id then
          _successor = node
          break
        end
      end

      if _successor is None then
        _successor = nodes(0)?  // Wrap around to the first node if needed
      end

      // Initialize finger table
      for i in Range(0, ChordConfig.node_bits()) do
        let target_id = (_id + (1 << i)) % ChordConfig.max_nodes()
        var found = false

        for node in nodes.values() do
          if node.id >= target_id then
            _finger_table(i)? = node  // Use partial assignment with `?`
            found = true
            break
          end
        end

        if not found then
          _finger_table(i)? = nodes(0)?  // Use partial assignment with `?`
        end
      end
    end

  be lookup(key: USize, p: Promise[(NodeInfo, USize)], hops: USize = 0) =>
    if key == _id then
      // Key is found at this node
      p((NodeInfo(_id, this), hops))
    else
      match _successor
      | let succ: NodeInfo =>
        if _in_range(key, _id, succ.id) then
          p((succ, hops + 1))
        else
          // Forward to the closest preceding node in the finger table
          try
            let next = _closest_preceding_node(key)?
            if next.id == _id then
              // Prevent infinite loop if we're stuck at the same node
              p((NodeInfo(_id, this), hops + 1))
            else
              next.actor_ref.lookup(key, p, hops + 1)
            end
          else
            // If no valid node found, return self
            p((NodeInfo(_id, this), hops + 1))
          end
        end
      else
        // If successor is not set, return self
        p((NodeInfo(_id, this), hops + 1))
      end
    end
    
  fun _closest_preceding_node(key: USize): NodeInfo ? =>
    // Traverse the finger table in reverse to find the closest preceding node
    for i in Range[USize](_finger_table.size() - 1, 0, -1) do
      let node = _finger_table(i)?
      if _in_range(node.id, _id, key) then
        return node
      end
    end
    // If no closer node is found, return this node’s successor if available
    _successor as NodeInfo

  fun _in_range(id: USize, start: USize, finish: USize): Bool =>
    if start < finish then
      (id > start) and (id <= finish)
    else
      (id > start) or (id <= finish)
    end

  be print_state() =>
    _env.out.print("Node " + _id.string())
    
    match _successor
    | let s: NodeInfo =>
      _env.out.print("  Successor: " + s.id.string())
    else
      _env.out.print("  No successor")
    end
    
    _env.out.print("  Finger table:")
    for (i, node) in _finger_table.pairs() do
      _env.out.print("    " + i.string() + ": " + node.id.string())
    end

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
      
      let actors = Array[Node](num_nodes)
      
      // Create nodes and build nodes array within recover block
      let nodes = recover val
        let arr = Array[NodeInfo](num_nodes)
        for i in Range[USize](0, num_nodes) do
          let id = (i * ChordConfig.max_nodes()) / num_nodes
          let node = Node(env, id)
          actors.push(node)
          arr.push(NodeInfo(id, node))
          env.out.print("Node " + id.string() + " created")
        end
        arr
      end
      
      // Initialize nodes
      for node in actors.values() do
        node.initialize(nodes)
      end

      // Perform lookup
      let p = Promise[(NodeInfo, USize)]
      if actors.size() > 0 then
        actors(0)?.lookup(key, p)
        
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
