// The Paxos flavors in reading order, each with the problem that it solves.
// The map of /paxos/ (`src/pages/paxos/index.astro`) lists them, and a
// bridge of the tour (`src/tour.ts`) opens the first chapter of each flavor
// with its problem line.

export interface Flavor {
  n: number;
  name: string;
  /** The trace and the page of the flavor. */
  page: string;
  path: string;
  problem: string;
  flavor: string;
}

export const FLAVORS: Flavor[] = [
  { n: 1, name: "Single-decree Paxos", page: "single-decree", path: "paxos/single-decree/",
    problem: "Many machines must agree on one value, and some of them fail.",
    flavor: "Acceptors choose one value. The choice never changes." },
  { n: 2, name: "Multi-Paxos", page: "multi-paxos", path: "paxos/multi-paxos/",
    problem: "A journal needs many values in order, and two round trips per record is slow.",
    flavor: "A log of slots. One leader runs Phase 1 once, then each write costs one round trip." },
  { n: 3, name: "Matchmaker Paxos", page: "matchmaker", path: "paxos/matchmaker/",
    problem: "Machines fail and get replaced while the journal takes writes.",
    flavor: "Matchmakers keep the acceptors of each ballot. A change of acceptors is a new ballot." },
  { n: 4, name: "Compartmentalized Paxos", page: "compartmentalized", path: "paxos/compartmentalized/",
    problem: "One leader does all the work, so one machine sets the throughput.",
    flavor: "The leader splits into roles that scale alone. The leader moves with no election." },
  { n: 5, name: "Quorums", page: "quorums", path: "paxos/quorums/",
    problem: "Every write touches a majority of the acceptors.",
    flavor: "The quorum system says which acceptors must answer each phase." },
];
