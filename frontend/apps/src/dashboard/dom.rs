use dominator::{clone, html, Dom};
use futures_signals::signal::SignalExt;
use super::Dashboard;
use crate::{ router::Router, common::header};

use std::sync::Arc;

impl Dashboard {
    pub fn render(self: &Arc<Self>) -> Dom {
        let state = self;
        html!("div", {
            .children(&mut [
                html!("div", {
                    .text("Dashboard")
                })
            ])
        })
    }
}