
// clang-format off 
/*
COMMON PART OF EVERY KIND OF NODE (INTERNAL OR LEAF):

 ┌────────────────────────┬─────────────────────────────────────────────┬─────────────────────┐
 │   verlib::versioned    │   flck::atomic_write_once<TS> time_stamp;   │                     │
 │                        │   verlib::versioned *next_version;          │                     │
 ├────────────────────────┼─────────────────────────────────────────────┼─────────────────────┤
 │   ordered_map::header  │   const node_type nt;                       │                     │
 │                        │   char size;                                │                     │
 │                        │   short int byte_num;                       │   K key;            │
 └────────────────────────┴─────────────────────────────────────────────┴─────────────────────┘


THE "NODE" CLASS, USED IN LEAVES, ADDING TO THE FIELDS ABOVE

 ┌────────────────────────┬─────────────────────────────────────────────┬─────────────────────┐
 │   ordered_map::node    │   verlib::atomic_bool removed;              │                     │
 │                        │   verlib::lock lck;                         │                     │
 └────────────────────────┴─────────────────────────────────────────────┴─────────────────────┘


FULL NODE:

 ┌───────────────────────────────┬─────────────────────────────────────────────┬─────────────────────┐
 │      verlib::versioned        │   flck::atomic_write_once<TS> time_stamp;   │                     │
 │                               │   verlib::versioned *next_version;          │                     │
 ├───────────────────────────────┼─────────────────────────────────────────────┼─────────────────────┤
 │     ordered_map::header       │   const node_type nt;                       │                     │
 │                               │   char size;                                │                     │
 │                               │   short int byte_num;                       │   K key;            │
 ├───────────────────────────────┼─────────────────────────────────────────────┼─────────────────────┤
 │      ordered_map::node        │   verlib::atomic_bool removed;              │                     │
 │                               │   verlib::lock lck;                         │                     │
 ├───────────────────────────────┼─────────────────────────────────────────────┼─────────────────────┤
 │    ordered_map::full_node     │   verlib::versioned_ptr children[256];      │                     │
 └───────────────────────────────┴─────────────────────────────────────────────┴─────────────────────┘


INDIRECT NODE:

 ┌───────────────────────────────┬─────────────────────────────────────────────┬─────────────────────┐
 │       verlib::versioned       │   flck::atomic_write_once<TS> time_stamp;   │                     │
 │                               │   verlib::versioned *next_version;          │                     │
 ├───────────────────────────────┼─────────────────────────────────────────────┼─────────────────────┤
 │      ordered_map::header      │   const node_type nt;                       │                     │
 │                               │   char size;                                │                     │
 │                               │   short int byte_num;                       │   K key;            │
 ├───────────────────────────────┼─────────────────────────────────────────────┼─────────────────────┤
 │      ordered_map::node        │   verlib::atomic_bool removed;              │                     │
 │                               │   verlib::lock lck;                         │                     │
 ├───────────────────────────────┼─────────────────────────────────────────────┼─────────────────────┤
 │  ordered_map::indirect_node   │   char idx[256];                            │                     │
 │                               │   verlib::versioned_ptr<node> ptr[64];      │                     │
 └───────────────────────────────┴─────────────────────────────────────────────┴─────────────────────┘
can you get away with persisting the ptrs ONLY LITERALLY



    SPARSE NODE:

 ┌───────────────────────────────┬─────────────────────────────────────────────┬─────────────────────┐
 │       verlib::versioned       │   flck::atomic_write_once<TS> time_stamp;   │                     │
 │                               │   verlib::versioned *next_version;          │                     │
 ├───────────────────────────────┼─────────────────────────────────────────────┼─────────────────────┤
 │      ordered_map::header      │   const node_type nt;                       │                     │
 │                               │   char size;                                │                     │
 │                               │   short int byte_num;                       │   K key;            │
 ├───────────────────────────────┼─────────────────────────────────────────────┼─────────────────────┤
 │      ordered_map::node        │   verlib::atomic_bool removed;              │                     │
 │                               │   verlib::lock lck;                         │                     │
 ├───────────────────────────────┼─────────────────────────────────────────────┼─────────────────────┤
 │   ordered_map::sparse_node    │   unsigned char keys[16];                   │                     │
 │                               │   verlib::versioned_ptr ptr[16];            │                     │
 └───────────────────────────────┴─────────────────────────────────────────────┴─────────────────────┘


LEAF:

 ┌───────────────────────────────┬─────────────────────────────────────────────┬─────────────────────┐
 │       verlib::versioned       │   flck::atomic_write_once<TS> time_stamp;   │                     │
 │                               │   verlib::versioned *next_version;          │                     │
 ├───────────────────────────────┼─────────────────────────────────────────────┼─────────────────────┤
 │      ordered_map::header      │   const node_type nt;                       │                     │
 │                               │   char size;                                │                     │
 │                               │   short int byte_num;                       │   K key;            │
 ├───────────────────────────────┼─────────────────────────────────────────────┼─────────────────────┤
 │   ordered_map::generic_leaf   │   KV key_vals[2 or 14];                     │                     │
 └───────────────────────────────┴─────────────────────────────────────────────┴─────────────────────┘

Summary of node type changes (in INSERT only, so far), which usually entail
creating a new node, copying the old node content to it with some modifications
(adding or removing a particular KV):

* When adding a new child:
    * Indirect -> Indirect: The parent is copied to a new indirect node, with
the new child added.
    * Indirect -> Full: The parent is copied to a new full node, with the new
child added.
    * Sparse -> Sparse: The parent is copied to a new sparse node, with the new
child added.
    * Sparse -> Indirect: The parent is copied to a new indirect node, with the
new child added.


* When child ptr exists, but the child itself is nullptr
    * Small leaf creation: A new small leaf is created, holding only the new
key-value pair to be inserted.

* When child ptr exists, and the child itself is a leaf
    * Small leaf -> Small leaf: The leaf is copied to a new small leaf, with the
new key-value pair added.
    * Small leaf -> Big leaf: The leaf is copied to a new big leaf, with the new
key-value pair added.
    * Big leaf -> Big leaf: The leaf is copied to a new big leaf, with the
key-value pair added.
    * Big leaf -> Sparse: A sparse node is created, and multiple leaves (small
or big) are created and attached to it.

* When child ptr exists, and the child is not a leaf
    * Small leaf creation: A new small leaf is created, holding only the new
key-value pair to be inserted.
    * Sparse node creation: A new sparse node is created, holding the new
key-value pair to be inserted.
    * Sparse/Indirect/Full -> Sparse: The newly created sparse node holds ptr to
the new leaf and the old node.

*/
// clang-format on

