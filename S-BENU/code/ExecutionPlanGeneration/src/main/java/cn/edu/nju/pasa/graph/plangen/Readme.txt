The incremental execution plans generation aims to generate the best incremental execution plans for a pattern graph.

Input the pattern graph to pattern.txt.
	first line:
		n m k
			n: number of nodes, m: number of edges, k: number of symmetry breaking conditions.
	followed by m lines, each line:
		x y: an undirected edge (x,y)
	followed by k lines, each line:
		a b: a < b, the partial order constaints "<" on a and b.
	note that the nodes must be labeled from 1 not 0.

Example input for triangle:
3 3 0
1 2
1 3
2 3