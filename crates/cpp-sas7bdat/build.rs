// build.rs
use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    // Get the output directory
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    
    let venv_bin_path = manifest_dir
            .parent()  // Go up one level from crates/cpp-sas7bdat
            .unwrap()
            .parent()  // Go up another level to the project root
            .unwrap()
            .join(".venv")
            .join("bin");
    println!("cargo:warning=manifest_dir ={:?}", manifest_dir.to_str());
    println!("cargo:warning=venv_bin_dir ={:?}", venv_bin_path.to_str());
    // Run uv sync first (assuming pyproject.toml exists)
    run_uv_sync(&manifest_dir);
    
    // Build cpp-sas7bdat using make
    build_cppsas7bdat(
        &manifest_dir,
        &venv_bin_path,
    );
    
    // Setup C++ compilation for our wrapper
    let mut build = cc::Build::new();
    
    // Basic C++ settings
    build
        .cpp(true)
        .std("c++17")
        .flag("-O3")
        .flag("-DNDEBUG");
    
    // Platform-specific settings
    if cfg!(target_os = "linux") {
        build.flag("-fPIC");
    }
    
    if cfg!(target_os = "macos") {
        build.flag("-stdlib=libc++");
    }
    
    // Include directories
    build
        .include("vendor/src")        // Remove cpp-sas7bdat/ prefix
        .include("vendor/include")    // Remove cpp-sas7bdat/ prefix
        .include("vendor/build/Release")
        .include("vendor/src/cpp")   // Your wrapper code location
        .define("SPDLOG_FMT_EXTERNAL", None);  // Tell spdlog to use external fmt
    
    // Add cpp-sas7bdat built library path
    let cppsas_build_dir = manifest_dir.join("vendor/build/Release/src");
    println!("cargo:rustc-link-search=native={}", cppsas_build_dir.display());
    
    // Find and link dependencies
    setup_dependencies(&mut build);
    
    // Add your wrapper source files
    build
        .file("vendor/src/cpp/chunked_reader.cpp")
        .file("vendor/src/cpp/c_api.cpp");
        ;
    
    // Compile wrapper
    build.compile("sas_chunked_wrapper");
    
    // Generate bindings
    generate_bindings(&out_dir);
    
    // Link libraries
    println!("cargo:rustc-link-lib=static=cppsas7bdat");
    
    // Link system dependencies based on what cpp-sas7bdat needs
    link_system_dependencies();
    
    // Tell cargo to re-run if source files change
    println!("cargo:rerun-if-changed=vendorsrc/cpp/");
    println!("cargo:rerun-if-changed=cpp-sas7bdat/");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=pyproject.toml");
}

fn run_uv_sync(manifest_dir: &Path) {
    println!("cargo:warning=Running uv sync...");
    
    let output = Command::new("uv")
        .arg("sync")
        .current_dir(manifest_dir)
        .output()
        .expect("Failed to run uv sync. Make sure uv is installed.");
    
    if !output.status.success() {
        panic!(
            "uv sync failed:\nstdout: {}\nstderr: {}", 
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    
    println!("cargo:warning=uv sync completed successfully");
}

fn build_cppsas7bdat(
    manifest_dir: &Path,
    venv_bin_dir: &Path,
) {
    let cppsas_dir = manifest_dir.join("vendor");
    //  println!("{}",&format!("{:?}",cppsas_dir));
    if !cppsas_dir.exists() {
        panic!("cpp-sas7bdat directory not found. Please ensure it's checked out as a submodule or dependency.");
    }
    
    println!("cargo:warning=Building cpp-sas7bdat...");
    
    
    // Check if we need to run cmake first
    let build_dir = cppsas_dir.clone();
    if !build_dir.exists() {
        std::fs::create_dir_all(&build_dir).expect("Failed to create build directory");
        
        // Run cmake
        let cmake_output = Command::new("cmake")
            .arg("..")
            .arg("-DCMAKE_BUILD_TYPE=Release")
            .arg("-DBUILD_SHARED_LIBS=OFF") // Build static library
            .current_dir(&cppsas_dir)
            .output()
            .expect("Failed to run cmake. Make sure cmake is installed.");
        
        if !cmake_output.status.success() {
            panic!(
                "cmake failed:\nstdout: {}\nstderr: {}", 
                String::from_utf8_lossy(&cmake_output.stdout),
                String::from_utf8_lossy(&cmake_output.stderr)
            );
        }
    }
    

    // Run make
    println!("build: {:?}",build_dir);
    let current_path = std::env::var("PATH").unwrap_or_default();
    let new_path = format!("{}:{}", venv_bin_dir.display(), current_path);

        // Add this before running make to debug the conan issue
    let debug_conan = Command::new("bash")
        .arg("-c")
        .arg("which conan && file $(which conan) && head -5 $(which conan)")
        .env("VIRTUAL_ENV", venv_bin_dir.parent().unwrap())
        .env("PATH", new_path.clone())
        .current_dir(&build_dir)
        .output()
        .expect("Failed to debug conan");

    println!("cargo:warning=Conan debug: {}", String::from_utf8_lossy(&debug_conan.stdout));
    println!("cargo:warning=Conan debug stderr: {}", String::from_utf8_lossy(&debug_conan.stderr));

    
    let make_output = Command::new("make")
        .arg("build")
        .env("VIRTUAL_ENV", venv_bin_dir.parent().unwrap())
        .env("PATH", new_path)
        .current_dir(&build_dir)
        .output()
        .expect("Failed to run make. Make sure make is installed.");
    
    if !make_output.status.success() {
        panic!(
            "make failed:\nstdout: {}\nstderr: {}", 
            String::from_utf8_lossy(&make_output.stdout),
            String::from_utf8_lossy(&make_output.stderr)
        );
    }
    
    println!("cargo:warning=cpp-sas7bdat build completed successfully");
}

fn setup_dependencies(build: &mut cc::Build) {
    // Read CMake cache first
    let cmake_cache_path = "vendor/build/Release/CMakeCache.txt";
    if let Ok(cmake_cache) = std::fs::read_to_string(cmake_cache_path) {
        extract_all_include_paths_from_cmake(&cmake_cache, build);
    }
    
    // Look for conan2 dependencies in home directory
    if let Ok(home_dir) = env::var("HOME") {
        let conan2_dir = format!("{}/.conan2", home_dir);
        if std::path::Path::new(&conan2_dir).exists() {
            find_and_link_all_conan_libraries();
        }
    }
}

fn find_and_link_all_conan_libraries() {
    if let Ok(home_dir) = env::var("HOME") {
        let conan2_dir = format!("{}/.conan2/p/b", home_dir);
        let mut found_libs = std::collections::HashSet::new();
        
        if let Ok(entries) = std::fs::read_dir(&conan2_dir) {
            for entry in entries.flatten() {
                let package_path = entry.path();
                let lib_path = package_path.join("p").join("lib");
                if lib_path.exists() {
                    println!("cargo:rustc-link-search=native={}", lib_path.display());
                    
                    if let Ok(lib_entries) = std::fs::read_dir(&lib_path) {
                        for lib_entry in lib_entries.flatten() {
                            let lib_name = lib_entry.file_name().to_string_lossy().to_string();
                            
                            if lib_name.ends_with(".a") && lib_name.starts_with("lib") {
                                if let Some(clean_name) = lib_name.strip_prefix("lib").and_then(|s| s.strip_suffix(".a")) {
                                    if !matches!(clean_name, "c" | "m" | "dl" | "pthread" | "rt" | "gcc" | "gcc_s" | "stdc++" | "util") {
                                        if !found_libs.contains(clean_name) {
                                            println!("cargo:rustc-link-lib=static={}", clean_name);
                                            found_libs.insert(clean_name.to_string());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        println!("cargo:warning=Linked {} Conan libraries", found_libs.len());
    }
}

fn extract_all_include_paths_from_cmake(cmake_cache: &str, build: &mut cc::Build) {
    for line in cmake_cache.lines() {
        // Extract include directories from various CMake variables
        if line.contains("INCLUDE_DIR") && line.contains("=") {
            if let Some(path) = line.split('=').nth(1) {
                if !path.is_empty() && std::path::Path::new(path).exists() {
                    build.include(path);
                    println!("cargo:warning=Added include path from CMake: {}", path);
                }
            }
        }
        
        // Also look for package paths
        if line.starts_with("spdlog_DIR:") || line.starts_with("Boost_DIR:") {
            if let Some(path) = line.split('=').nth(1) {
                // These usually point to lib/cmake/PackageName, so go up to include
                std::path::Path::new(path)
                    .parent()
                    .and_then(|p| p.parent())
                    .and_then(|p| p.parent())
                    .map(|package_root| {
                        let include_dir = package_root.join("include");
                        if include_dir.exists() {
                            build.include(&include_dir);
                            println!("cargo:warning=Added package include: {}", include_dir.display());
                        }
                    });
            }
        }
    }
}
fn generate_bindings(out_dir: &Path) {
    let bindings = bindgen::Builder::default()
        .header("vendor/src/cpp/c_api.h")
        .clang_arg("-Ivendor/src")      // Remove cpp-sas7bdat/ prefix
        .clang_arg("-Ivendor/include")  // Remove cpp-sas7bdat/ prefix
        .clang_arg("-Ivendor/src/cpp")
        .clang_arg("-std=c++17")
        // Tell bindgen we're using C++
        .clang_arg("-x")
        .clang_arg("c++")
        // Generate bindings for your C API
        .allowlist_function("chunked_reader_.*")
        .allowlist_function("chunk_iterator_.*")
        .allowlist_function("free_.*") 
        .allowlist_type(".*")  // Allow all types
        .allowlist_var(".*")
        // Generate the bindings
        .generate()
        .expect("Unable to generate bindings");
    
    bindings
        .write_to_file(out_dir.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}

fn link_system_dependencies() {
    // Link system libraries based on platform
    if cfg!(target_os = "linux") {
        println!("cargo:rustc-link-lib=dl");
        println!("cargo:rustc-link-lib=pthread");
        println!("cargo:rustc-link-lib=stdc++");
    } else if cfg!(target_os = "macos") {
        println!("cargo:rustc-link-lib=c++");
        println!("cargo:rustc-link-lib=System");
    } else if cfg!(target_os = "windows") {
        // Windows-specific libraries if needed
        println!("cargo:rustc-link-lib=msvcrt");
    }
}