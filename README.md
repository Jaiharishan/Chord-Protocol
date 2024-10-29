# Project README

## Team Members
### Member 1
- **Name**    : Vishal Karthikeyan Setti 
- **UFID**    : 47670880
- **E-Mail**  : v.setti@ufl.edu

### Member 2
- **Name**    : Jaiharishan Arunagiri Veerakumar
- **UFID**    : 62333614
- **E-Mail**  : j.arunagiriveera@ufl.edu

## What is Working
- **Chord Protocol Implementation**: Successfully implemented the Chord protocol for a peer-to-peer (P2P) network using hashing and a finger table. This implementation allows for efficient distributed hash table operations, such as key lookups, by leveraging consistent hashing to distribute keys evenly across nodes. The finger table optimizes the search process, enabling O(logn) complexity for lookups by maintaining a list of potential successors for each node which is also in O(logn) complexity.

  ![Screenshot 2024-10-29 021624](https://github.com/user-attachments/assets/db91fbf9-50b1-4c5f-9702-8abe60b015dc)


## Largest Network Managed
- The  largest network managed to deal is 2^32 count, which is around 4 billion count.

## Usage
To run the program, use the following command format:
.\Chord-Protocol numNodes numRequests

**Example**:
.\Chord-Protocol 8 10
