#!/usr/bin/env python
"""
Interactive setup wizard for the MIMIC deterioration pipeline
"""

import sys
import os
from pathlib import Path


def print_header(text):
    """Print a formatted header"""
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70 + "\n")


def check_python_version():
    """Check Python version"""
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required")
        print(f"   Current version: {sys.version}")
        return False
    print(f"✅ Python version: {sys.version.split()[0]}")
    return True


def check_dependencies():
    """Check if required packages are installed"""
    print("\nChecking dependencies...")
    
    required = [
        "psycopg2",
        "yaml",
        "pandas",
        "numpy",
        "tqdm",
        "dotenv",
        "pytest"
    ]
    
    missing = []
    for package in required:
        try:
            __import__(package.replace("-", "_"))
            print(f"  ✅ {package}")
        except ImportError:
            print(f"  ❌ {package} - NOT INSTALLED")
            missing.append(package)
    
    if missing:
        print(f"\n⚠️  Missing packages: {', '.join(missing)}")
        print("   Install with: pip install -r requirements.txt")
        return False
    
    return True


def check_env_file():
    """Check if .env file exists"""
    env_path = Path(".env")
    
    if not env_path.exists():
        print("\n⚠️  .env file not found")
        create = input("   Create .env file now? (y/n): ").strip().lower()
        
        if create == 'y':
            password = input("   Enter PostgreSQL password: ").strip()
            
            with open(".env", "w") as f:
                f.write(f"PGPASSWORD={password}\n")
            
            print("   ✅ .env file created")
            return True
        else:
            print("   ℹ️  You'll need to create .env manually")
            return False
    else:
        print("\n✅ .env file exists")
        # Check if password is set
        with open(".env", "r") as f:
            content = f.read()
            if "PGPASSWORD=" in content and "your_password_here" not in content.lower():
                print("   ✅ Password appears to be set")
                return True
            else:
                print("   ⚠️  Password may not be set correctly")
                return False


def check_config():
    """Check if config files exist"""
    print("\nChecking configuration files...")
    
    configs = [
        "config/config.yaml",
        "config/outcomes.yaml",
        "config/feature_catalog.yaml"
    ]
    
    all_ok = True
    for cfg in configs:
        if Path(cfg).exists():
            print(f"  ✅ {cfg}")
        else:
            print(f"  ❌ {cfg} - NOT FOUND")
            all_ok = False
    
    return all_ok


def run_validation():
    """Run the validation script"""
    print("\n" + "=" * 70)
    run = input("Run full validation checks now? (y/n): ").strip().lower()
    
    if run == 'y':
        print("\nRunning validation...")
        os.system("python validate_setup.py")
    else:
        print("\nℹ️  Run validation later with: python validate_setup.py")


def run_tests():
    """Run the test suite"""
    print("\n" + "=" * 70)
    run = input("Run test suite now? (y/n): ").strip().lower()
    
    if run == 'y':
        print("\nRunning tests...")
        os.system("python -m pytest tests/ -v")
    else:
        print("\nℹ️  Run tests later with: python -m pytest tests/ -v")


def main():
    """Main setup wizard"""
    print_header("Cardiac Deterioration Pipeline - Setup Wizard")
    
    print("This wizard will help you set up the pipeline.\n")
    
    # Step 1: Check Python version
    print_header("Step 1: Python Version")
    if not check_python_version():
        print("\n❌ Setup cannot continue. Please upgrade Python.")
        return False
    
    # Step 2: Check dependencies
    print_header("Step 2: Dependencies")
    if not check_dependencies():
        print("\n⚠️  Please install missing dependencies first:")
        print("   pip install -r requirements.txt")
        
        install = input("\nTry to install now? (y/n): ").strip().lower()
        if install == 'y':
            print("\nInstalling dependencies...")
            os.system(f"{sys.executable} -m pip install -r requirements.txt")
            print("\nDependencies installed. Please run this wizard again.")
        return False
    
    # Step 3: Check .env file
    print_header("Step 3: Environment Configuration")
    check_env_file()
    
    # Step 4: Check config files
    print_header("Step 4: Configuration Files")
    config_ok = check_config()
    
    if not config_ok:
        print("\n❌ Configuration files are missing!")
        return False
    
    # Step 5: Run validation
    print_header("Step 5: Validation")
    run_validation()
    
    # Step 6: Run tests
    print_header("Step 6: Tests")
    run_tests()
    
    # Final instructions
    print_header("Setup Complete!")
    
    print("Next steps:")
    print("\n1. Verify your database settings in config/config.yaml")
    print("2. Ensure MIMIC-IV data is loaded in PostgreSQL")
    print("3. Run the pipeline:")
    print("   python -m src.main")
    print("\n4. Check outputs:")
    print("   - Datasets: artifacts/datasets/")
    print("   - Logs: artifacts/logs/")
    print("\n5. For help, see:")
    print("   - README.md (full documentation)")
    print("   - QUICKSTART.md (quick start guide)")
    print("   - IMPLEMENTATION_SUMMARY.md (technical details)")
    
    print("\n" + "=" * 70)
    print("✅ Setup wizard complete!")
    print("=" * 70 + "\n")
    
    return True


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Setup wizard cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Setup wizard error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
