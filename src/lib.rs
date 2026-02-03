use prost::Message;
use std::fs;

// Include generated protobuf code
pub mod person {
    include!(concat!(env!("OUT_DIR"), "/person.rs"));
}

use person::{AddressBook, Person, person::PhoneNumber, person::PhoneType};

#[unsafe(no_mangle)]
pub extern "C" fn add(left: i32, right: i32) -> i32 {
    // Create a Person
    let person = Person {
        name: "Maria Garcia".to_string(),
        id: 1234,
        email: "maria.garcia@example.com".to_string(),
        phone_numbers: vec!["555-1234".to_string(), "555-5678".to_string()],
        phones: vec![PhoneNumber {
            number: "555-9999".to_string(),
            r#type: PhoneType::Mobile as i32,
        }],
        is_active: true,
    };

    println!("Created Person:");
    println!("Name: {}", person.name);
    println!("ID: {}", person.id);
    println!("Email: {}", person.email);

    // Serialize to bytes
    let mut buf = Vec::new();
    person.encode(&mut buf).unwrap();
    println!("\nSerialized to {} bytes", buf.len());

    // Deserialize from bytes
    let decoded = Person::decode(&buf[..]).unwrap();
    println!("\nDeserialized Person:");
    println!("Name: {}", decoded.name);
    println!("Active: {}", decoded.is_active);

    // Save to file
    fs::write("person.bin", &buf).unwrap();
    println!("\nSaved to person.bin");

    // Read from file
    let file_data = fs::read("person.bin").unwrap();
    let from_file = Person::decode(&file_data[..]).unwrap();
    println!("Read from file: {}", from_file.name);

    // Create AddressBook
    let address_book = AddressBook {
        people: vec![
            person,
            Person {
                name: "Jane Smith".to_string(),
                id: 5678,
                email: "jane@example.com".to_string(),
                phone_numbers: vec![],
                phones: vec![],
                is_active: true,
            },
        ],
    };

    println!("\nAddress book has {} people", address_book.people.len());

    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
