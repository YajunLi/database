# Yajun Li 2019.6.27
################################################################
# Database indexing, the B-tree index
################################################################
# the only thing developers need to learn is how to index
# “An index makes the query fast”
################################################################
# The separation of concerns—what is needed versus how to get it—
# works remarkably well in SQL, but it is still not perfect

ie = """SELECT date_of_birth
  FROM employees
 WHERE last_name = 'WINAND'"""

# An index is a distinct structure in the database that is built
# using the create index statement. It requires its own disk
# space and holds a copy of the indexed table data.

# an index is pure redundancy
# it does not change the table data
# it refers to the actual information stored in a different place

##################################################################
# the index leaf nodes, logical order v.s. physical order
##################################################################
# The primary purpose of an index is to provide an ordered
# representation of the indexed data. It is, however, not possible
# to store the data sequentially because an insert statement would
# need to move the following entries to make room for the new one.
# Moving large amounts of data is very time-consuming so the insert
# statement would be very slow. The solution to the problem is to
# establish a logical order that is independent of physical order in
# memory.

# It is possible to insert new entries without moving large amounts
# of data—it just needs to change some pointers.

# The logical order is established via a doubly linked list.
# Every node has links to two neighboring entries, very much like a chain.
# New nodes are inserted between two existing nodes by updating their links
# to refer to the new node. The physical location of the new node doesn't
# matter because the doubly linked list maintains the logical order.

# The doubly linked lists is used to connect the so-called index leaf nodes.
# Each leaf node is stored in a database block or page; that is,
# the database's smallest storage unit. All index blocks are of the same size
# —typically a few kilobytes.

# table structure:
# Unlike the index, the table data is stored in a heap structure and is not
# sorted at all. There is neither a relationship between the rows stored in
# the same table block nor is there any connection between the blocks.

############################################################################
# The Search Tree (B-Tree) Makes the Index Fast
############################################################################
# The index leaf nodes are stored in an arbitrary order—the position on the
# disk does not correspond to the logical position according to the index
# order.
# It is like a telephone directory with shuffled pages. If you search
# for “Smith” but first open the directory at “Robinson”, it is by no
# means granted that Smith follows Robinson. A database needs a second
# structure to find the entry among the shuffled pages quickly: a balanced
# search tree—in short: the B-tree.

# the structure of the balanced search tree:
# The doubly linked list establishes the logical order between the leaf nodes.
# The root and branch nodes support quick searching among the leaf nodes.
# Each branch node entry corresponds to the biggest value in the respective leaf node.

############################################################################
# The B-tree enables the database to find a leaf node quickly.
# the first power of indexing--the tree traversal is a very efficient operation
# It works almost instantly—even on a huge data set. That is primarily
# because of the tree balance, which allows accessing all elements with the
# same number of steps, and secondly because of the logarithmic growth of
# the tree depth. That means that the tree depth grows very slowly compared
# to the number of leaf nodes.
# Real world indexes with millions of records have a tree depth of four or five.
# A tree depth of six is hardly ever seen.
############################################################################
# slow index:
# (1) the tree traversal;
# (2) following the leaf node chain;
# (3) fetching the table data.
# The tree traversal is the only step that has an upper bound for the number
# of accessed blocks—the index depth. The other two steps might need to
# access many blocks—they cause a slow index lookup.


























