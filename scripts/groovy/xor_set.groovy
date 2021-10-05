class MyClass {
  String foobar_string
  boolean foobar_bool
  ArrayList foobar_list

  def isSet(arg) {
      if(this."${arg}") return true else return false
  }

  def isSetExclusive(args) {
      def found = false
      for (arg in args) {
        if (isSet(arg) && found) { found = false; break }
        else found = found || isSet(arg)
      }
      return found
  }
}

def fields = ["foobar_string", "foobar_bool", "foobar_list"]

def asdf_class = new MyClass(foobar_string: "asdf", foobar_bool: true, foobar_list: ["asdf"])
fields.each { f -> assert asdf_class.isSet(f)}

testCases = [
  [
    [
      foobar_string: "asdf",
      foobar_bool: true,
      foobar_list: ["asdf"]
    ],
    false
  ],
  [
    [
      foobar_string: "asdf"
    ],
    true
  ],
  [
    [
      foobar_bool: true
    ],
    true
  ],
  [
    [
      foobar_list: ["asdf"]
    ],
    true
  ],
]

testCases.each { c ->
  def input = c[0]
  def output = c[1]
  println "Testing MyClass(${input}).isSetExclusive(${fields}) -> ${output}"
  def test_class = new MyClass(input)
  assert test_class.isSetExclusive(fields) == output
  println "PASS"
  println ""
}

// Testing MyClass([foobar_string:asdf, foobar_bool:true, foobar_list:[asdf]]).isSetExclusive([foobar_string, foobar_bool, foobar_list]) -> false
// PASS
//
// Testing MyClass([foobar_string:asdf]).isSetExclusive([foobar_string, foobar_bool, foobar_list]) -> true
// PASS
//
// Testing MyClass([foobar_bool:true]).isSetExclusive([foobar_string, foobar_bool, foobar_list]) -> true
// PASS
//
// Testing MyClass([foobar_list:[asdf]]).isSetExclusive([foobar_string, foobar_bool, foobar_list]) -> true
// PASS


class ScriptStub {
  def echo(m) {
    println m
  }
}


class MyClass2 {
  Object script = new ScriptStub()
  String foobar_string
  boolean foobar_bool  // test comment
  ArrayList foobar_list

  MyClass2(Map kwargs, Map defaults=[:], ArrayList required=[]) {
      /**
       * Custom map constructor for extended classes.
       *
       * Default values set directly on the class members don't work for extended
       * versions of this class. Subsequently, the children must pass a map of defaults.
       *
       * @param kwargs: a map of variables to setup the artifact
       * @param defaults: a map of default values for the artifact kwargs
       * @param required: a list of kwargs which must be set by the end user
       */
      def m = defaults
      m.putAll(kwargs)
      m.each { k, v -> this."${k}" = v }
      def missing = []
      def isSet = { a -> (this."${a}") ? true : false }
      def isSetExclusive = { args ->
        def found = false
        for (arg in args) {
          if (isSet(arg) && found) { found = false; break }
          else found = found || isSet(arg)
        }
        return found
      }
      required.each { k ->
        try {
          if (k instanceof String) assert isSet(k)
          if (k instanceof ArrayList) assert isSetExclusive(k)
        } catch(AssertionError e) { missing.add(k) }
      }
      if (missing) {
          missing.each { k ->
            if (k instanceof String) this.script.echo("You must set ${k}")
            if (k instanceof ArrayList)this.script.echo("You must set one of the following: ${k}")
          }
          throw new Exception("Missing the following paramters: ${missing}")
      }
  }
}

// class MyClass2Subclass extends MyClass2 {
//     MyClass2Subclass(Map m) {
//         super(m, [:], ["sentryProject"])
//     }

testCases = [
  [
    [
      foobar_string: "asdf",
      foobar_bool: true,
      foobar_list: ["asdf"]
    ],
    fields,
    true
  ],
  [
    [
      foobar_string: "asdf",
      foobar_bool: true,
      foobar_list: ["asdf"]
    ],
    fields,
    false
  ],
  [
    [
      foobar_string: "asdf"
    ],
    ["foobar_string", fields],
    true
  ],
  [
    [
      foobar_bool: true
    ],
    [fields],
    true
  ],
  [
    [
      foobar_list: ["asdf"]
    ],
    [fields],
    true
  ],
  [
    [
      foobar_bool: true,
      foobar_list: ["asdf"]
    ],
    [fields],
    true
  ],
]

testCases.each { c ->
  def input = c[0]
  def required = c[1]
  def output = c[2]
  println "Testing MyClass(${input})"
  def test_class = new MyClass2(*:input, [:], required)
  // assert test_class.isSetExclusive(fields) == output
  // println "PASS"
  // println ""
}
