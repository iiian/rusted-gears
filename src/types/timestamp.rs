use chrono::DateTime;
use chrono::Utc;
use tokio_postgres::types::Type;

use tokio_postgres::types::FromSql;

use serde::Deserialize;

use serde::de::Visitor;

use serde::Serialize;

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Default, Debug)]
pub(crate) struct TimestampWithTimeZone(DateTime<Utc>);

// rest of code implements serde + SQL conversion

impl Serialize for TimestampWithTimeZone {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.0.format("%Y-%m-%dT%H:%M:%S.000Z").to_string())
    }
}

pub(crate) struct StringVisitor;

impl<'de> Visitor<'de> for StringVisitor {
    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("is it really that hard?")
    }

    type Value = &'de str;

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(v)
    }
}

impl<'de> Deserialize<'de> for TimestampWithTimeZone {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let x = deserializer.deserialize_str(StringVisitor)?;
        #[allow(deprecated)]
        Ok(TimestampWithTimeZone(DateTime::from_utc(
            DateTime::parse_from_rfc3339(x).ok().unwrap().naive_utc(),
            Utc,
        )))
    }
}

impl ToString for TimestampWithTimeZone {
    fn to_string(&self) -> String {
        let k = self.0;
        k.to_string()
    }
}

impl<'a> FromSql<'a> for TimestampWithTimeZone {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if ty.name() == "timestamp" {
            let naive_datetime = chrono::NaiveDateTime::from_sql(ty, raw)?;
            #[allow(deprecated)]
            Ok(TimestampWithTimeZone(DateTime::from_utc(
                naive_datetime,
                Utc,
            )))
        } else {
            Err("Unexpected column type".into())
        }
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "timestamp"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn deser_datetime() {
        let dt = DateTime::parse_from_rfc3339("1964-05-10T12:32:16Z")
            .unwrap()
            .to_utc();
        let my_dtwtz = TimestampWithTimeZone(dt);
        let payload = serde_json::json!(my_dtwtz);

        let serialized = serde_json::to_string(&payload).unwrap();
        assert_eq!(serialized, "\"1964-05-10T12:32:16.000Z\"");
        let deserialized: TimestampWithTimeZone = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized, my_dtwtz);
    }
}
