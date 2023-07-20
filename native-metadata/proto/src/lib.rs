pub mod proto{
    pub mod entity {
        include!(concat!(env!("OUT_DIR"), "/proto.entity.rs"));
    }
}