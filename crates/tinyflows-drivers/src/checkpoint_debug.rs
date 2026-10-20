use super::DriverCheckpointer;

impl<State> std::fmt::Debug for DriverCheckpointer<State> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DriverCheckpointer")
            .field("ns", &self.ns)
            .finish_non_exhaustive()
    }
}
