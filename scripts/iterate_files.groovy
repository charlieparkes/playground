// source: https://stackoverflow.com/questions/3953965/get-a-list-of-all-the-files-in-a-directory-recursive

import groovy.io.FileType

def dir = new File("./scripts")
dir.eachFileRecurse(FileType.FILES) { file ->
    println file
}
