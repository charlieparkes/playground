def x = []
if (x instanceof ArrayList) println "x was an ArrayList"

def args = ["asdf", ["foo", "bar"]]
args.each { a ->
  if (a instanceof String) println "${a} was a String"
  if (a instanceof ArrayList) println "${a} was an ArrayList"
}
