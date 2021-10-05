# Challenge #321 [Hard] Circle Splitter
https://www.reddit.com/r/dailyprogrammer/comments/6ksmh5/20170630_challenge_321_hard_circle_splitter/

## Problem
You've got a square in 2D space with axis values between 0-1 which is filled
with points. Can you place a circle within the square such that exactly half
of the points in the square lie within the circle?

    * Circle may touch the edge of the square but may not pass outside.
    * Points on the edge of the circle count as inside.
    * There will always be an even number of points.
    * You must indicate if it is unsolvable.

## Solution
I take every unique combination of three and two points to find valid circles and check if they contain half the points. If a circle is out of bounds or has a radius larger than the best solution found so far, we don't bother checking it.

Generate input and check output: https://jsfiddle.net/gjkdc8hL/
