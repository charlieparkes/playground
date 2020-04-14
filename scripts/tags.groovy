ArrayList tags = "doc-viewer@0.0.2,doc-viewer@0.0.3,product-line-tagger-common@0.0.2,product-line-tagger@0.0.2".trim().split(",")
Map tagsByApp = [:].withDefault { key -> [] }

tags.each { t ->
    def _split = t.split("@")
    if (_split.size() == 2) {
        def app_name = _split[0]
        def tag = _split[1]
        if (tag[0] != "v") tag = "v${tag}"
        tagsByApp[app_name].add(tag)
    }
}
tagsByApp.each { name, _tags ->
    println "${name}: ${_tags.sort().reverse()}"
}
