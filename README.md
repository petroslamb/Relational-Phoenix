Relational-Phoenix
==================

Relational operators on top of Phoenix 2

Computer Science M.Sc. 2012, The University of Edinburgh

Please read `Thesis.pdf` for the full text of the dissertation.

## About

This project realizes some common relational operators on top of [Phoenix](https://github.com/kozyraki/phoenix).

Phoenix is a shared-memory implementation of Google's MapReduce model for data-intensive processing tasks. Phoenix can be used to program multi-core chips as well as shared-memory multiprocessors (SMPs and ccNUMAs). 

Here I have written the MapReduce jobs over Phoenix's C API needed to implement the most common relational operators: 

```
Select, Project, Sort, Aggregate (but not yet Join)
```

The project is a proof of concept abstraction layer over Phoenix's MapReduce API, simplifying its usage while keeping the MapReduce details concealed to the user. 
