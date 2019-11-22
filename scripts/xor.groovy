def xor(l) {
    def found = false
    for (x in l) {
        if (x && found) { found = false; break }
        else found = found || x
    }
    return found
}

def testCases = [
  [
    [true, false, false],
    true
  ],
  [
    [false, false, false],
    false
  ],
  [
    [true, true],
    false
  ],
  [
    [true, false, true],
    false
  ],
]

testCases.each { c ->
  def input = c[0]
  def output = c[1]
  println "Testing xor(${input}) -> ${output}"
  assert xor(input) == output
  println "PASS"
  println ""
}

// Testing xor([true, false, false]) -> true
// PASS
//
// Testing xor([false, false, false]) -> false
// PASS
//
// Testing xor([true, true]) -> false
// PASS
//
// Testing xor([true, false, true]) -> false
// PASS


testCases = [
  [
    "",
    false
  ],
  [
    false,
    false
  ],
  [
    [],
    false
  ],
  [
    "asdf",
    true
  ],
  [
    true,
    true
  ],
  [
    ["asdf"],
    true
  ],
]

def isSet(x) {
    if(x) return true else return false
}

testCases.each { c ->
  def input = c[0]
  def output = c[1]
  println "Testing isSet(${input}) -> ${output}"
  assert isSet(input) == output
  println "PASS"
  println ""
}

// Testing isSet() -> false
// PASS
//
// Testing isSet(false) -> false
// PASS
//
// Testing isSet([]) -> false
// PASS
//
// Testing isSet(asdf) -> true
// PASS
//
// Testing isSet(true) -> true
// PASS
//
// Testing isSet([asdf]) -> true
// PASS
