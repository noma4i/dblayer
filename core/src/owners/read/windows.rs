//! Owner `Windows`: a dictionary of `Window` machines by key.
//! A table row applies to the machine of the key an input names. Inputs that name a request (`Response`, `Failure`, `Dropped`, `RetryDue`,
//! `Committed`, `Rejected`) go to the machine that owns the request. `Online`, `EpochStarted`, `EpochEnded`, `ModelsReset`, `Closing` apply to
//! every machine, `Stale` to the machines of its model named in `keys`. Request ids are unique in the owner. The retry timer is `Effect::Schedule`
//! of the owner (`after` = the backoff). Inputs for other owners (`Commit`, `WindowState`) are `Effect::Send` to the addresses of the state.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use statig::prelude::*;

use crate::owners::read::deliveries::{DeliveryInput, Deliveries};
use crate::owners::store::{StoreBatch, StoreInput, StoreOp, StoreWriter};
use crate::runtime::RetryPolicy;
use crate::owners::contract::{BatchAuthor, Committed, Drained, EpochEnded, EpochStarted, ErrorClass, ModelsReset, Online, RejectReason, Rejected, RequestError, RequestId, WindowPhase as Phase, WindowState};
use crate::runtime::{Address, Cause, Effect, EffectId, Effects, Epoch, Outbound, ScheduleId, StateOwner, Time};

include!("windows_types.rs");

include!("windows_facts.rs");

include!("windows_flow.rs");

include!("windows_owner.rs");
