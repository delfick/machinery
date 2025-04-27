Machinery
=========

This is a collection of functionality.

Docs found at https://machinery.readthedocs.io

History
-------

Between June 2016 and July 2021 I (@delfick) worked at LIFX and created the
private version of https://photons.delfick.com (with the open source version
coming out in March 2018.

In this project I created a bunch of utilities, especially around using ``asyncio``
code without creating memory leaks or annoying warnings when tasks aren't awaited
before the end of the program.

In November 2023 I started this project to extract those helpers so they can be
used independently of Photons and so I could make them strongly typed and tested
better.

In April 2025, the initial wave of those helpers were rewritten to be strongly
typed with a slightly different approach to a number of those concepts.
