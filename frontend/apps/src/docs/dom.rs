use dominator::{clone, html, Dom};
use futures_signals::signal::SignalExt;
use super::Docs;
use crate::{router::Router, common::header};

use std::sync::Arc;

impl Docs {
    pub fn render(self: &Arc<Self>) -> Dom {
        let state = self;
        html!("div", {
            .children(&mut [
                html!("div", {
                    .text("Docs")
                })
            ])
            
        })
    }
}