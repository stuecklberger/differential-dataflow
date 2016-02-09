extern crate rand;
extern crate time;
extern crate timely;
extern crate differential_dataflow;

use std::hash::{Hash, SipHasher, Hasher};

use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::progress::timestamp::RootTimestamp;

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::collection::LeastUpperBound;
use differential_dataflow::operators::join::JoinBy;
use differential_dataflow::operators::group::GroupBy;

type Node = u32;
type Edge = (Node, Node, u64);
type RoutingTableEntry = (Node, Node, Node, Node);
type ThroughRule = (Node, Node, Node);

// enum Rule {
//  Through(Node, Node, Node),
// }

fn main() {

    let nodes: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: u32 = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: u32 = std::env::args().nth(3).unwrap().parse().unwrap();
    let rules: u32 = std::env::args().nth(4).unwrap().parse().unwrap();

    // defines a new computational scope in which to run Dijkstra's algorithm
    timely::execute_from_args(std::env::args().skip(5), move |computation| {

        let start = time::precise_time_s();

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);        // rng for edge additions
        let mut rng2: StdRng = SeedableRng::from_seed(seed);        // rng for edge deletions

        let other_seed: &[_] = &[7, 8, 9, 10];
        let mut rng3: StdRng = SeedableRng::from_seed(other_seed);  // rng for rule creation

        println!("Making up {} random rules", rules);

        let mut random_rules = Vec::new();
        if computation.index() == 0 {
            for index in 0..rules {
                let mut counter = 0;
                loop { // loop until a valid rule was added
                    counter = counter + 1;
                    if counter % 10000 == 0 { println!("iterated 10000 times!"); }

                    let s = rng3.gen_range(0, nodes);
                    let i = rng3.gen_range(0, nodes);
                    let d = rng3.gen_range(0, nodes);

                    if s != d && s != i && i != d { // check for valid rule
                        random_rules.push(((s, i, d), 1));
                        println!("Rule {}: {} == {} ==> {}", index, s, i, d);
                        break; // break after adding the rule
                    }
                }
            }
        }

        println!("Initializing computation");

        // defines dataflow; returns handles to edges input and root
        let (mut graph, probe, mut rules) = computation.scoped(|scope| {

            // set all nodes in the graph as destination nodes
            let dests = (0..nodes).map(|n| (n,1)).to_stream(scope);
            let (edge_input, edge_input_node) = scope.new_input();
            // let rulesStream = random_rules.map(|(a, b, c)| ((a, b, c), 1)).to_stream(scope); 
            let (rule_input, rule_input_node) = scope.new_input(); // dynamic rule input (necessary?)
            let routing_table = get_routing_table_entries(&Collection::new(edge_input_node), &Collection::new(dests), &Collection::new(rule_input_node));

            //.inspect(|&((n,d,f),_w)| println!("Forwarding rule: ({},{}) -> {}",n,d,f));

            (edge_input, routing_table.probe().0, rule_input)
        });

        println!("performing Dijkstra on {} nodes, {} edges:", nodes, edges);

        if computation.index() == 0 {
            // add edges
            for _ in 0..(edges/1000) {
                for _ in 0..1000 {
                    let s = rng1.gen_range(0, nodes);
                    let d = rng1.gen_range(0, nodes);
                    let w = rng1.gen_range(0, nodes) as u64;

                    graph.send(((s,d,w), 1));
                    graph.send(((d,s,w), 1));
                }
                computation.step();
            }

            // add rules
            while let Some(rule) = random_rules.pop() {
                rules.send(rule);
            }
            rules.close();
        }

        println!("loaded; elapsed: {}s", time::precise_time_s() - start);

        graph.advance_to(1);
        while probe.le(&RootTimestamp::new(0)) { computation.step(); }

        println!("stable; elapsed: {}s", time::precise_time_s() - start);

        if batch > 0 {
            let mut changes = Vec::new();
            for wave in 0.. {
                for _ in 0..batch {
                    changes.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes),rng1.gen_range(0, nodes) as u64), 1));
                    changes.push(((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes),rng2.gen_range(0, nodes) as u64),-1));
                }

                let start = time::precise_time_s();
                let round = *graph.epoch();
                if computation.index() == 0 { // changes are made by the first worker
                    while let Some(change) = changes.pop() {
                        graph.send(change);
                    }
                }
                graph.advance_to(round + 1);
                while probe.le(&RootTimestamp::new(round)) { computation.step(); }

                if computation.index() == 0 {
                    println!("wave {}: avg {}", wave, (time::precise_time_s() - start) / (batch as f64));
                }
            }
        }
    });
}

// (n, dest, through, fwd)

// returns tuples of the form (n, dest, fwd) indicating that the shortest path between n and dest goes through node fwd which is one hop away from n
// in our use case, each such tuple defines a forwarding rule of the form (n,dest)->fwd, assuming a shortest-path routing protocol
fn get_routing_table_entries<G: Scope>(edges: &Collection<G, Edge>, dests: &Collection<G, Node>, rules: &Collection<G, ThroughRule>)
-> Collection<G, RoutingTableEntry>
where G::Timestamp: LeastUpperBound {

    // dests reach themselves at distance 0 (through x, fwd x)
    // (id, dest, through, weight, fwd)
    let nodes = dests.map(|x| (x, x, x, 0u64, x));

    // repeatedly update minimal distances each node can be reached from each root
    nodes.iterate(|inner| {

        let edges = edges.enter(&inner.scope());
        let nodes = nodes.enter(&inner.scope());
        let rules = rules.enter(&inner.scope());




                                        // join on rules through x
                                        // x ==[w]==> y (through 'through', fwd 'fwd')
                                        // and rule: a ==> x ==> dest
                                        // implies
                                        // x ==[w]==> y (through 'x', fwd 'fwd')
        let additional_throughs = inner.join_by(
                                            &rules,
                                            |(x,dest,through,w,fwd)| ((x,dest),(through,w,fwd)),
                                            |(a,x,dest)| ((x,dest),a),
                                            hash,
                                            |&(x,dest),&(_,w,fwd),&_| (x,dest,x,w,fwd)
                                        );




              // y ==[w2]==> x ==[w1]==> dest (through 'through')
              // implies
              // y ==[w1+w2]==> dest (through 'through', fwd x)
        inner.join_by_u(
                &edges,
                |(x,dest,through,w1,fwd)| (x,(dest,through,w1,fwd)),
                |(y,x,w2)| (x,(y,w2)),
                |&x, &(dest,through,w1,_), &(y,w2)| (y,dest,through,w1+w2,x)
              )

               // x ==[0]==> x (through x, fwd x)
              .concat(&nodes)

               // x ==[w]==> dest (through 'through', fwd 'fwd')
               // and rule: a ==> x ==> dest
               // implies
               // x ==[w]==> dest (through 'x', fwd 'fwd')
              .concat(&additional_throughs)


              // for each (n,dest,through) return the one with the lowest w, if same lowest fwd
               // n ==[w]==> dest (fwd to 'fwd', through 'through')
              .map( |(n,dest,through,w,fwd)| ((n,dest,through),(w,fwd)) )
              .group( |_,v,out| out.push( (*v.peek().unwrap().0,1) ) )
              .map( |((n,dest,through),(w,fwd))| (n,dest,through,w,fwd) )

              // .group_by_u(
              //   |(n,dest,through,w,fwd)| (n,(dest,through,w,fwd)),
              //   |&n,&(dest,through,w,fwd)| (n,dest,through,w,fwd),
              //   |_, v, out| {
              //    let mut dest = std::u32::MAX;
              //    let mut through = std::u32::MAX;
              //    for (val,_) in v {// get the shortest path through each relevant node to each destination node
              //        if val.0==dest && val.1==through { continue; }
              //        dest = val.0;
              //        through = val.1;
              //        out.push((*val, 1));            
              //    }
              //   }
              // )
    })
    //.inspect(|&((n,d,w,f),_w)| println!("Forwarding rule: ({},{},{}) -> {}",n,d,w,f))
    .map(|(n,dest,through,_,fwd)| (n,dest,through,fwd))
}

fn hash<T: Hash>(x: &T) -> u64 {
    let mut h = SipHasher::new();
    x.hash(&mut h);
    h.finish()
}