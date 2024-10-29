use "time"
use "collections"
use "random"
use "math"

class ChordNotify is TimerNotify
    let _node: Node
    let _task: String
    var _env: Env
    var _id: U64
  new iso create(env:Env, node: Node, id: U64, task: String) =>
    _env = env
    _node = node
    _task = task
    _id = id

  fun ref apply(timer: Timer, count: U64): Bool =>
    match _task
    | "stabilize" =>
        _node.stabilize()
    | "fix_fingers" =>
        _node.fix_fingers()
    | "check_predecessor" =>
        _node.check_predecessor()
    else
      _env.out.print("Unknown task " + _task)
    end
    true

actor Node
  let _env: Env
  var _id: U64
  var _successor: Node
  var _predecessor: Node
  var _successor_id: U64
  var _predecessor_id: U64
  var successor_stable_rounds: U64 = 0
  var predecessor_stable_rounds: U64 = 0
  var finger_table_stable_rounds: U64 = 0
  var _finger_table: Array[(U64,Node)]
  var _previous_finger_table: Array[(U64,Node)]
  var _next_finger: USize
  var _m: USize
  var _timer: Timer tag
  var _stabilize_timer: Timer tag
  var _predecessor_check_timer: Timer tag
  var _data: Map[U64, String] iso
  let _main: Main
  let timers: Timers
  var _stabilized: Bool

  new create(env: Env, main: Main, id: U64, m: USize) =>
    timers = Timers
    _env = env
    _id = id
    _m = m
    _previous_finger_table = Array[(U64, Node)](_m)
    _finger_table = Array[(U64, Node)](_m)
    for i in Range[USize](0, _m) do
      _finger_table.push((_id, this))
      _previous_finger_table.push((_id,this))
    end
    _next_finger = 0
    _predecessor = this
    _successor = this
    _data = Map[U64,String]
    _successor_id = id
    _predecessor_id = id
    _main = main
    _stabilized = false

    let stabilize_interval:U64 = 1_00_000_000
    let stabilize_notify = ChordNotify(_env, this, _id, "stabilize")
    let stabilize_timer' = Timer(consume stabilize_notify, 1_000_000_000, stabilize_interval)
    _stabilize_timer = stabilize_timer'
    timers(consume stabilize_timer')

    let fix_fingers_interval:U64 = 1_000_0
    let fix_fingers_notify = ChordNotify(_env, this, _id, "fix_fingers")
    let fix_fingers_timer = Timer(consume fix_fingers_notify, 1_000_000_000, fix_fingers_interval)
    _timer = fix_fingers_timer
    timers(consume fix_fingers_timer)

    let check_predecessor_interval:U64 = 1_00_000_0000
    let check_predecessor_notify = ChordNotify(_env, this, _id, "check_predecessor")
    let check_predecessor_timer = Timer(consume check_predecessor_notify, 1_000_000_000, check_predecessor_interval)
    _predecessor_check_timer = check_predecessor_timer
    timers(consume check_predecessor_timer)

  be join(node: Node) =>
    find_successor(_id, node, "find_successor")

  be receive_successor(successor: Node, successor_id: U64) =>
    _successor = successor
    _successor_id = successor_id

  fun ref check_finger_table_stabilization()? =>
    var is_stabilized = true

    for i in Range[USize](0, _m) do
      let current_entry = _finger_table(i)?
      let previous_entry = _previous_finger_table(i)?

      let required_id: U64 = (_id + (1 << i.u64()).u64()) % (1 << _m).u64()

      if (current_entry._1 != previous_entry._1) or (current_entry._1 < required_id) then
        is_stabilized = false
        break
      end
    end

    if is_stabilized then
      finger_table_stable_rounds = finger_table_stable_rounds + 1
      check_stabilization()
    else
      finger_table_stable_rounds = 0

      for i in Range[USize](0, _m) do
        _previous_finger_table(i)? = _finger_table(i)?
      end
    end

  be find_successor(id: U64, requestor: Node, purpose: String = "find_successor", hop_count: U64 = 0, finger_index: USize = USize.max_value()) =>
    if in_range(id, _id, _successor_id) then
      match purpose
      | "find_successor" =>
        requestor.receive_successor(_successor, _successor_id)
      | "lookup" =>
        _env.out.print("New Node id: " + id.string() + " in range with purpose: " + purpose.string())
        try
          let value = _data(id)?
          requestor.rcv_lookup_result(id, value, hop_count)
        else
          requestor.rcv_lookup_result(id, "None", hop_count )
        end
      | "update_finger" =>
        if finger_index != USize.max_value() then
          requestor.update_finger(finger_index, _successor, _successor_id)
        else
          _env.out.print("Finger index not specified for finger table update.")
        end
      else
        _env.out.print("Unknown purpose in find_successor.")
      end
    else
      var closest_node = closest_preceding_node(id)
      if closest_node is this then
        closest_node  = _predecessor
      end
        match purpose
        | "lookup" =>
            closest_node.find_successor(id, requestor, purpose, hop_count + 1)
        | "update_finger" =>
            closest_node.find_successor(id, requestor, purpose, 0, finger_index)
        | "find_successor" =>
            closest_node.find_successor(id, requestor, purpose)
        end
    end

  be perform_key_lookup(key: U64, requestor: Node, hop_count: U64 = 0) =>
    if in_range(key, _predecessor_id, _id) then
      try
        let value = _data(key)?
        requestor.rcv_lookup_result(key, value, hop_count)
      else
        requestor.rcv_lookup_result(key, "None", hop_count)
      end
    else

      let closest_node = closest_preceding_node(key)
      if closest_node is this then
        _successor.perform_key_lookup(key, requestor, hop_count + 1)
      else
        closest_node.perform_key_lookup(key, requestor, hop_count + 1)
      end
    end

  be lookup(key: U64) =>
    perform_key_lookup(key, this, 0)

  be rcv_lookup_result(key: U64, value: String, hops: U64) =>
    if value is "None" then
      _env.out.print("Lookup result for key " + key.string() + " not found after " + hops.string() + " hops.")
    else
      _main.rcv_hop_count(_id, hops)
    end

  be print_finger_table() =>
    _env.out.print("Finger table for node " + _id.string() + ":")
    for i in Range[USize](0, _m) do
      try
        let finger_entry = _finger_table(i)?
        let finger_id: U64 = finger_entry._1
        _env.out.print("Finger " + i.string() + ", Node ID = " + finger_id.string())
      else
        _env.out.print("Error accessing finger table at index " + i.string())
      end
    end

  be update_finger(finger_index: USize, node: Node, id: U64) =>
    try
      _finger_table(finger_index)? = (id, node)
    else
      _env.out.print("Index not found!!")
    end

  be fix_fingers() =>
    if _next_finger >= _m then
      _next_finger = 0
    end

    let target_key: U64 = (_id + (1 << _next_finger).u64()) % (1 << _m).u64()
    find_successor(target_key, this, "update_finger", 0, _next_finger)
    _next_finger = _next_finger + 1

  fun ref closest_preceding_node(id: U64): Node =>
    var i: I64 = _finger_table.size().i64() - 1

    while i >= 0 do
      try
        let finger: (U64, Node) = _finger_table(i.usize())?

          if in_range(finger._1, _id, id) then
            return finger._2
          end
      else
        _env.out.print("Error accessing keys or finger table. Continuing...")
      end

      i = i - 1
    end

    this

  be lookupkey()=>
    None

  be store_key(key: U64, value: String) =>
    _data(key) = value

  fun in_range(id: U64, id_start: U64, id_end: U64): Bool =>
    if id_start < id_end then
      (id > id_start )and (id <= id_end)
    else
      (id > id_start) or (id <= id_end)
    end

  be check_predecessor() =>
    _predecessor.alive(this)

  be alive(response_to: Node) =>
    None

  fun ref check_stabilization() =>
    if ((successor_stable_rounds >= 2) and (predecessor_stable_rounds >= 2)) and (not _stabilized) then
      _stabilized = true
      _main.node_stabilized(_id)
    end

  be notify(caller: Node, caller_id: U64) =>
      if in_range(caller_id, _predecessor_id, _id) then
        _predecessor = caller
        _predecessor_id = caller_id
        predecessor_stable_rounds = 0
      else
        predecessor_stable_rounds = predecessor_stable_rounds +  1
      end
      check_stabilization()

  be stabilize() =>
    _successor.request_predecessor(this)

  be receive_predecessor(pred: Node, pred_id: U64) =>
      if in_range(pred_id, _id, _successor_id) then
        _successor = pred
        _successor_id = pred_id
        successor_stable_rounds = 0
      else
        successor_stable_rounds = successor_stable_rounds + 1
      end

      _successor.notify(this, _id)
      check_stabilization()

  be request_predecessor(requestor: Node) =>
    requestor.receive_predecessor(_predecessor, _predecessor_id)

  fun ref final()=>
    _env.out.print("Deleting node: "+_id.string())
    
  be stop() =>
    timers.cancel(_timer)
    timers.cancel(_stabilize_timer)
    timers.cancel(_predecessor_check_timer)


actor Main
  let _env: Env
  var numNodes: U64
  var numRequests: U64
  var numKeys: U64
  var nodes_map: Map[U64, Node tag] = Map[U64, Node tag]
  var _rand: Rand
  var total_hops: U64 = 0
  var total_requests: U64 = 0
  let timers : Timers = Timers
  let node_ids: Array[U64]
  let temp_array: Array[U64]
  let initial_data: Map[U64, String]
  var all_keys: MinHeap[U64]
  var nodes_stabilized:Array[U64]

  new create(env: Env) =>
    _env = env
    _rand = Rand(Time.now()._2.u64())
    numNodes = 0
    numRequests = 0
    numKeys = 0
    node_ids = Array[U64]
    temp_array = Array[U64]
    initial_data = Map[U64, String]
    all_keys = MinHeap[U64](10)
    nodes_stabilized = Array[U64]
    if env.args.size() != 3 then
      env.out.print("Usage: p2p <numNodes> <numRequests>")
      return
    end

    try
      numNodes = env.args(1)?.u64()?
      numRequests = env.args(2)?.u64()?
      numKeys = 2 * numNodes
      all_keys = MinHeap[U64](numKeys.usize())
      env.out.print("Number of Nodes: " + numNodes.string())
      env.out.print("Number of Requests: " + numRequests.string())
      env.out.print("Number of Keys: " + numKeys.string())

    try
      generate_nodes(numNodes, numKeys)?
    else
      env.out.print("Error getting keys")
    end
      let notify = Notifier(this, _env)
      let timer = Timer(consume notify, 5_000_000_000, 0)
    else
      env.out.print("Error: Unable to parse arguments.")
    end

  fun ref generate_nodes(num_nodes: U64, num_keys: U64)? =>
      let m: USize = 32
      let id_space: U64 = (1 << m.u64()) - 1
      let keys_per_node = num_keys / num_nodes
      var nodes_list: Array[(U64, Node tag)] = Array[(U64, Node tag)](num_nodes.usize())
      var max_id: U64 = 0
      var node_ids_set: Set[U64] = Set[U64]()

      for i in Range[U64](0, num_nodes) do
        var node_id: U64 = _rand.int_unbiased(id_space)

        while node_ids_set.contains(node_id) do
          node_id = _rand.int_unbiased(id_space)
        end
        
        let node: Node tag = Node(_env, this, node_id, m)
        nodes_list.push((node_id, node))
        max_id = max_id.max(node_id)
        _env.out.print("Node ID: " + node_id.string())
        nodes_map.update(node_id, node)
        node_ids_set.add(node_id)
      end

      let bootstrap_index = _rand.int_unbiased(num_nodes)
      let bootstrap_node: Node = nodes_list(bootstrap_index.usize())?._2
      let bootstrap_node_id: U64 = nodes_list(bootstrap_index.usize())?._1
      // _env.out.print("Selected bootstrap node with ID: " + bootstrap_node_id.string())

      for i in Range[U64](0, nodes_list.size().u64()) do
        let node_id: U64 = nodes_list(i.usize())?._1
        let node: Node = nodes_list(i.usize())?._2
        if node_id != bootstrap_node_id then
          bootstrap_node.join(node)
        end
      end

      var all_keys_set: Set[U64] = Set[U64]()

      for i in Range[U64](0, num_keys) do
        var key: U64 = _rand.int_unbiased(max_id)

        while all_keys_set.contains(key) do
          key = _rand.int_unbiased(max_id)
        end
        
        all_keys.push(key)
        temp_array.push(key)
        initial_data(key) = "File"+key.string()
        all_keys_set.add(key)
      end

      for key in nodes_map.keys() do
        node_ids.push(key)
      end

      Sort[Array[U64], U64](node_ids)

      for i in Range[U64](0, node_ids.size().u64()) do
        try
          let current_node_id = node_ids(i.usize())?

          while (all_keys.size() > 0) and (all_keys.peek()? <= current_node_id) do
            let k: U64 = all_keys.peek()?
            nodes_map(current_node_id)?.store_key(k, initial_data(k)?)
            _env.out.print("Key : " + k.string() + " in Node ID: " + current_node_id.string())
            all_keys.pop()?
          end
        else
          _env.out.print("Key or value not found!")
        end
      end

      while all_keys.size() > 0 do
        let k: U64 = all_keys.pop()?
        let first_node_id = node_ids(0)?
        nodes_map(first_node_id)?.store_key(k, initial_data(k)?)
        _env.out.print("Fallback assignment - Key: " + k.string() + " in Node_id: " + first_node_id.string())
      end

  be lookup() =>
    try 
      for node_id in nodes_map.keys() do
        let node: Node = nodes_map(node_id)?
        for _ in Range[U64](0, numRequests) do
          let random_key:U64 = temp_array((_rand.u64() % temp_array.size().u64()).usize())?
          _env.out.print("Key " + random_key.string() + " present in Node " + node_id.string())
          node.lookup(random_key)
        end
      end
    else
      _env.out.print("[Lookup]Key index Out of bound")
    end

  fun ref calculate_avg_hops() =>
    if total_requests > 0 then
      let average_hops:F64 = (total_hops.f64() / total_requests.f64()).f64()
      _env.out.print("Average hops per lookup: " + average_hops.string())
    else
      _env.out.print("No requests completed.")
    end

  be rcv_hop_count(node_id:U64, hops: U64) =>
    total_hops = total_hops + hops
    total_requests = total_requests + 1
    _env.out.print("Node id: "+ node_id.string() +",Hop Count: " + hops.string())

    if total_requests >= (numNodes * numRequests) then
      for node in nodes_map.values() do
        node.stop()
      end
      calculate_avg_hops()
    end

  be node_stabilized(node_id: U64) =>
    nodes_stabilized.push(node_id)
    if nodes_stabilized.size().u64() == numNodes then
      _env.out.print("Chord network has been fully stabilized.")
      lookup()
    end

class Notifier is TimerNotify
  let _main: Main
  let _env: Env

  new iso create(main: Main, env: Env) =>
    _main = main
    _env = env

  fun ref apply(timer: Timer, count: U64): Bool =>
    _env.out.print("Notifier triggered.")
    true