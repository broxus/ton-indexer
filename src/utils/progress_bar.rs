pub struct ProgressBar {
    log_level: log::Level,
    name: &'static str,
    percentage_step: u64,
    current: u64,
    total: Option<u64>,
    exact_unit: Option<&'static str>,
}

impl ProgressBar {
    pub fn builder(name: &'static str) -> ProgressBarBuilder {
        ProgressBarBuilder::new(name)
    }

    pub fn set_total(&mut self, total: impl Into<u64>) {
        self.total = Some(total.into());
    }

    pub fn set_progress(&mut self, current: impl Into<u64>) {
        let old = self.compute_current_progress();
        self.current = current.into();
        let new = self.compute_current_progress();

        if matches!(
            (old, new),
            (Some(old), Some(new)) if old / self.percentage_step != new / self.percentage_step
        ) {
            self.progress_message();
        }
    }

    pub fn complete(&self) {
        self.message("complete");
    }

    #[inline(always)]
    fn progress_message(&self) {
        let total = match self.total {
            Some(total) if total > 0 => total,
            _ => return,
        };

        let percent = self.current * 100 / total;
        match self.exact_unit {
            Some(exact_unit) => self.message(format_args!(
                "{percent}% ({} / {total} {exact_unit})",
                self.current
            )),
            None => self.message(format_args!("{percent}%")),
        }
    }

    #[inline(always)]
    fn message(&self, text: impl std::fmt::Display) {
        log::log!(self.log_level, "{}... {text}", self.name);
    }

    fn compute_current_progress(&self) -> Option<u64> {
        self.total
            .filter(|&total| total > 0)
            .map(|total| self.current * 100u64 / total)
            .map(From::from)
    }
}

pub struct ProgressBarBuilder {
    log_level: log::Level,
    name: &'static str,
    percentage_step: u64,
    total: Option<u64>,
    exact_unit: Option<&'static str>,
}

impl ProgressBarBuilder {
    pub fn new(name: &'static str) -> Self {
        Self {
            log_level: log::Level::Info,
            name,
            percentage_step: PERCENTAGE_STEP,
            total: None,
            exact_unit: None,
        }
    }

    pub fn log_level(mut self, log_level: log::Level) -> Self {
        self.log_level = log_level;
        self
    }

    pub fn percentage_step(mut self, step: u64) -> Self {
        self.percentage_step = std::cmp::max(step, 1);
        self
    }

    pub fn total(mut self, total: impl Into<u64>) -> Self {
        self.total = Some(total.into());
        self
    }

    pub fn exact_unit(mut self, unit: &'static str) -> Self {
        self.exact_unit = Some(unit);
        self
    }

    pub fn build(self) -> ProgressBar {
        let pg = ProgressBar {
            log_level: self.log_level,
            name: self.name,
            percentage_step: self.percentage_step,
            current: 0,
            total: self.total,
            exact_unit: self.exact_unit,
        };

        if self.total.is_some() {
            pg.progress_message();
        } else {
            pg.message("estimating total");
        }

        pg
    }
}

const PERCENTAGE_STEP: u64 = 5;
