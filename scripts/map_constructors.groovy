import groovy.transform.*

// @MapConstructor
// @ToString
// class Person {
//     String name
//     String alias
//     List likes
// }
//
// def alias = "cmathews"
// def name = "Charlie Mathews"
// def likes = ["foo", "bar"]
//
// def p =
//     new Person(
//         alias: alias,
//         name: name,
//         likes: likes)
//
// assert p.alias == alias
// assert p.name == name
// assert p.likes == likes
//
// println p
//
// def p_args = [
//   alias: alias,
//   name: name,
//   likes: likes
// ]
//
// def p2 = new Person(p_args)
//
// assert p2.alias == alias
// assert p2.name == name
// assert p2.likes == likes
//
// println p2

@ToString(includeNames=true)
abstract class AbstractPerson implements Serializable {
    String script
    String explicit_environment = null
    boolean skipChecks = false
    ArrayList parallelStages = []

    AbstractPerson(Map m, ArrayList required=[]) {

        // Define kwargs which must be set when initializing via a map constructor.
        def REQUIRED_KWARGS = ["script"] + required

        // -- DANGER ZONE ---------------------------------------------------------

        m.each { k, v -> this."${k}" = v }
        def missing = []
        REQUIRED_KWARGS.each { k -> try { assert this."${k}" != null } catch(AssertionError e) { missing.add(k) } }
        if(missing) throw new Exception("Missing the following paramters: ${missing}")
    }
}

// @ToString
class Person extends AbstractPerson {
    ArrayList serviceNames
    boolean skipChecks = false
    ArrayList parallelStages = ["Deploy"]
    String asdf

    Person(Map m) {
        super(m, ["serviceNames"])
    }
}

def ap_args = [
  serviceNames: ["foo", "bar"],
  //script: "{{SCRIPT}}",
  skipChecks: false,
]
// def ap = new Person(ap_args)
// println ap


// def ct = Person
// def ap2 = ct.newInstance(ap_args)
// println ap2


def add(String clsName, Map args){
    args["script"] = "{{SCRIPT}}"
    cls = Class.forName(clsName,  false, Thread.currentThread().contextClassLoader)
    return cls.newInstance(args)
}
p = add("Person", ap_args)
println p

println "list of artifacts"

Class cls = Person
List<AbstractPerson> artifacts = []
artifacts << cls.newInstance(ap_args)
artifacts.each { a -> println a}

def getList() {
  return [1, 2, 3]
}
getList().each { x -> println x }
if (!getList().contains(4)) println "list doesn't contain 4"


@ToString(includeNames=true)
abstract class AbstractArtifact implements Serializable {
    String releaseEnvironment = null
    boolean skipChecks = false
    ArrayList parallelStages = []

    AbstractArtifact(Map kwargs, Map defaults, ArrayList required=[]) {
        def m = defaults
        m.putAll(kwargs)
        m.each { k, v -> this."${k}" = v }
        def missing = []
        required.each { k -> try { assert this."${k}" != null } catch(AssertionError e) { missing.add(k) } }
        if (missing) throw new Exception("Missing the following paramters: ${missing}")
    }
}

@ToString(includeNames=true, includeSuper=true)
class EcsService extends AbstractArtifact {
    ArrayList serviceNames
    String dockerfile
    String repositoryName
    String testImageName
    String clusterType
    Map buildArgs = [:]
    Map options = [:]
    int deployTimeout
    boolean skipDockerLinting

    ArrayList parallelStages = ["Deploy"]
    ArrayList allowedClusterTypes = ['cpu', 'general', 'fargate', 'infra']

    EcsService(Map m) {
        super(m, [
          dockerfile: "docker/Dockerfile",
          clusterType: "general",
          buildArgs: [:],
          options: [:],
          deployTimeout: 180,
          skipDockerLinting: false
        ], ["serviceNames"])
        if (!repositoryName) repositoryName = serviceNames.first()
    }
}

def x = new EcsService([
        serviceNames: ["logstash"],
        dockerfile: "Dockerfile",
        clusterType: "infra",
        deployTimeout: 1200,
        releaseEnvironment: "shared"
    ])

assert x.serviceNames == ["logstash"]
assert x.dockerfile == "Dockerfile"
assert x.clusterType == "infra"
assert x.deployTimeout == 1200
assert x.releaseEnvironment == "shared"

println x

def __str__(a) {
    def spacer = ", "
    def properties = []
    a.metaClass.properties.each { properties.add(it.name + ":" + a."${it.name}") }
    println "${a.getClass()}(${properties.join(spacer)})"
}

__str__(x)
