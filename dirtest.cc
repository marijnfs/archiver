#include <glib.h>
#include <glib-2.0/gio/gio.h>
#include <iostream>

#include <string>

using namespace std;

string path = "./";

struct StringException : public std::exception {
  std::string str;
  StringException(std::string msg_) : str(msg_){}
  
  const char *what() const noexcept {
    return str.c_str();
  }
};

void enumerate(GFile *root, GFile *file);

int main(int argc, char **argv) {
  if (argc != 2) {
    cerr << "no path given" << endl;
    return -1;
  }
  
  GFile *file = g_file_new_for_path(argv[1]);
  enumerate(file, file);
}

void enumerate(GFile *root, GFile *file) {
  GFileEnumerator *enumerator;
  GError *error = NULL;

  enumerator =
    g_file_enumerate_children (file, "*",
                               G_FILE_QUERY_INFO_NOFOLLOW_SYMLINKS, NULL,
                               &error);

  if (error != NULL) {
    throw StringException(error->message);
  }
  while (TRUE)
    {
      GFile *child;
      GFileInfo *finfo;
      char *relative_path;

      if (!g_file_enumerator_iterate (enumerator, &finfo, &child, NULL, &error))
        break;
      if (!finfo)
        break;

      if (g_file_info_get_file_type (finfo) == G_FILE_TYPE_DIRECTORY)
        {
          enumerate(root, child);
          continue;
        }
      
      gchar *data = 0;
      gsize len(0);
      if (!g_file_get_contents (g_file_get_path(child),
                          &data,
                          &len,
                                &error))
        throw StringException("read error");
      
      g_free(data);
      //cout << g_file_info_get_name(finfo) << endl;
      
      relative_path = g_file_get_relative_path (root, child);
      cout << relative_path << " " << len << endl;
      g_assert (relative_path != NULL);
      g_free (relative_path);
      
      
    }
  g_file_enumerator_close(enumerator, NULL, &error);
}
