use super::AccountRequestProcessor;
use super::browser_logins::BrowserLogins;
use crate::processor_task_retirement::ProcessorTaskDrain;
use crate::processor_task_retirement::ProcessorTasks;
use codex_login::LoginRetirementReport;
use std::io;
use tokio::time::Instant;

/// Partial account-login evidence.  Each retained browser and wrapper task is
/// independently joined; lower resources are represented by their own
/// result-bearing retirement report.
#[derive(Clone, Debug)]
pub(crate) struct AccountLoginReport {
    pub tasks: ProcessorTaskDrain,
    pub browsers: Result<Vec<Option<LoginRetirementReport>>, ()>,
    pub admission_unavailable: bool,
}

impl AccountLoginReport {
    pub(crate) fn is_clean(&self) -> bool {
        !self.admission_unavailable
            && self.tasks.is_clean()
            && self.browsers.as_ref().is_ok_and(|reports| {
                reports.iter().all(|report| {
                    report
                        .as_ref()
                        .is_none_or(LoginRetirementReport::is_complete)
                })
            })
    }
}

#[derive(Clone)]
pub(crate) struct AccountLoginShutdown {
    deadline: Instant,
    tasks: ProcessorTasks,
    browsers: BrowserLogins,
    admission_unavailable: bool,
}

impl AccountRequestProcessor {
    pub(crate) fn begin_login_shutdown(
        &self,
        deadline: Instant,
        tasks: &ProcessorTasks,
    ) -> io::Result<AccountLoginShutdown> {
        let mut original = self
            .login_deadline
            .lock()
            .map_err(|_| io::Error::other("account login shutdown unavailable"))?;
        let deadline = *original.get_or_insert(deadline);
        self.login_shutdown.cancel();
        // Neither failed gate may prevent the other population being closed.
        let tasks_closed = tasks.close_registration();
        let browsers_closed = self.browser_logins.close(deadline);
        Ok(AccountLoginShutdown {
            deadline,
            tasks: tasks.clone(),
            browsers: self.browser_logins.clone(),
            admission_unavailable: tasks_closed.is_err() || browsers_closed.is_err(),
        })
    }
}

impl AccountLoginShutdown {
    pub(crate) fn deadline(&self) -> Instant {
        self.deadline
    }

    pub(crate) async fn wait(&self) -> AccountLoginReport {
        let (tasks, browsers) = tokio::join!(
            self.tasks.shutdown_until(self.deadline),
            self.browsers.wait(),
        );
        AccountLoginReport {
            tasks,
            browsers: browsers.map_err(|_| ()),
            admission_unavailable: self.admission_unavailable,
        }
    }
}

#[cfg(test)]
#[path = "login_shutdown_tests.rs"]
mod tests;
