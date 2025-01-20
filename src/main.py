import os
from Helpers.utils import Utils
from traceback import format_exc


# load our environment variables :) 
try: 
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass


def main(testing=False):
    
    try:
        # Load the request from the JSON file
        request_filename = "request.json"
        request = Utils.load_request(request_filename)
        service_name = request.get("service_name", "").lower()
        input_pauses_allowed = not os.getenv("HEADLESS_MODE")

        try:

            traverse = request.get("traverse", True)
            upload_files = request.get("upload_files", True)

            # Optionally, clean the paths file if you're trying to start fresh
            # Clean paths file for this go-around
            # paths_filename = "paths.json"
            # Utils.clean_paths(paths_filename)

            # Get the service class based on the service name, default to Windows if not provided in request.json
            service_class_name = Utils.get_service(service_name)

            # Initialize the service with the request
            service = service_class_name(request)

            # Traverse the directories and log the files
            if traverse:
                print("Traversing directories and logging files...")
                service.traverse()
                print("We have finished traversing the directories.")
                print("Please review the paths we have found and filter out any paths you do not need migrated.")
                print("Once you are ready to proceed, please click \"Confirm\" to begin the migration process.")

            # Upload all currently logged files
            # TODO: This is placeholder code and will need to be replaced with db logic instead of the logger.
            files_to_upload = service.logger.get_docs_that_need_uploading()
            if upload_files and files_to_upload:
                num_of_files_found = len(files_to_upload)
                print(f"Found {num_of_files_found} files to upload.")
                print("Uploading files...")

                service.upload_all_currently_logged_files()
                print("This program has finished the migration process.")

            print("Done!")

            if input_pauses_allowed:
                input("Press Enter to close this window.")

        except Exception as e:
            print("An error has occured.")
            print(format_exc())
            if input_pauses_allowed:
                input("Press Enter to close this window.")
            return 

    except Exception as e:
        print(format_exc())
        print("Failed to load request.json information.")
        if not os.getenv("HEADLESS_MODE"):
            input("Press Enter to close this window.")


if __name__ == "__main__":
    # if you wanna test it locally, uncomment out the following lines to make your life easier. :) 
    # # If we are in test mode, set the service name to Windows and set the root directory to "Testing"
    # request_filename = "request.json"
    # request = Utils.modify_request(request_filename, "service_name", "windows")
    # request = Utils.modify_request(request_filename, "root_directories", ["Testing"])

    main()
