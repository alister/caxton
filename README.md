# Caxton - large-scale demo/test of Redis-Graph

Created with `composer create-project symfony/skeleton caxton`

Named after a character in the book [Stranger in a Strange Land](https://en.wikipedia.org/wiki/Stranger_in_a_Strange_Land).

Creates (via Faker) a large number (100-10,000) of type :Persons, who have
:Tags (up to 15 each). Then a subset of the know Person's are linked with 
up to 50% of the others.

This simulates a small group linked to a larger set, and the amount of space
required for all is shown, as well as time taken for a number of queries.
