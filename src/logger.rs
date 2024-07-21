use crate::config::LOG_LEVEL;
use log::{Level, LevelFilter, Metadata, Record}; // 引入 log 库的相关模块和类型。
use std::env; // 引入标准库的 env 模块，用于读取环境变量。 // 引入配置文件中的 LOG_LEVEL 常量。

// 定义 Logger 结构体。
struct Logger;

// 初始化日志系统。
pub fn init() {
    static LOGGER: Logger = Logger;
    log::set_logger(&LOGGER).unwrap();

    let log_level: String = env::var(LOG_LEVEL).unwrap_or_else(|_| String::from("INFO"));
    log::set_max_level(match log_level.as_str() {
        "ERROR" => LevelFilter::Error,
        "WARN" => LevelFilter::Warn,
        "INFO" => LevelFilter::Info,
        "DEBUG" => LevelFilter::Debug,
        "TRACE" => LevelFilter::Trace,
        _ => LevelFilter::Info,
    });
}

impl Logger {
    /// 根据日志级别从环境变量中获取颜色，如果未设置则使用默认颜色。
    fn get_color_for_level(level: Level) -> u8 {
        match level {
            Level::Error => env::var("LOG_COLOR_ERROR")
                .unwrap_or_else(|_| "31".to_string())
                .parse()
                .unwrap_or(31),
            Level::Warn => env::var("LOG_COLOR_WARN")
                .unwrap_or_else(|_| "93".to_string())
                .parse()
                .unwrap_or(93),
            Level::Info => env::var("LOG_COLOR_INFO")
                .unwrap_or_else(|_| "34".to_string())
                .parse()
                .unwrap_or(34),
            Level::Debug => env::var("LOG_COLOR_DEBUG")
                .unwrap_or_else(|_| "32".to_string())
                .parse()
                .unwrap_or(32),
            Level::Trace => env::var("LOG_COLOR_TRACE")
                .unwrap_or_else(|_| "90".to_string())
                .parse()
                .unwrap_or(90),
        }
    }
}

// 为 Logger 实现 log::Log trait，以便与 log 库兼容。
impl log::Log for Logger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }

        let color = Logger::get_color_for_level(record.level());

        println!(
            "\u{1B}[{}m[{:>5}]: {} - {}\u{1B}[0m",
            color,
            record.level(),
            record.target(),
            record.args(),
        );
    }

    fn flush(&self) {}
}
