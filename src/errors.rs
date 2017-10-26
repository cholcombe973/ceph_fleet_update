error_chain! {
    types {
        Error, ErrorKind, ResultExt, Result;
    }
    foreign_links {
        Io(::std::io::Error);
        Tarpc(::tarpc::Error<E>);
    }
    errors {
        CephError(t: String) {
            description("Ceph error")
            display("Ceph error: '{}'", t)
        }
    }
}
