/******************************************************************************/
/*                                                                            */
/*                       x r o o t d - f s u i d . c                          */
/*                                                                            */
/* (C) Copyright 2026 CERN.                                                   */
/******************************************************************************/

/*
  Root trampoline for FsOss: drop to a unix user, keep CAP_SETUID/CAP_SETGID
  (and CAP_NET_BIND_SERVICE) in the ambient set, then exec xrootd.

  Named xrootd-fsuid because xrootdfs is the FUSE client.

  sudo xrootd-fsuid -u xyz -- xrootd -c /path/cfg
  sudo xrootd-fsuid xrootd -R xyz -c /path/cfg
*/

#define _GNU_SOURCE

#include <errno.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifdef __linux__
#include <linux/capability.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#endif

#ifndef PR_CAP_AMBIENT
#define PR_CAP_AMBIENT 47
#define PR_CAP_AMBIENT_RAISE 2
#endif

static void Usage(int rc)
{
   fprintf(stderr,
      "Usage: xrootd-fsuid [-u user] [--] command [args...]\n"
      "\n"
      "Must start as root. Drops to <user>, keeps CAP_SETUID, CAP_SETGID and\n"
      "CAP_NET_BIND_SERVICE, then execs command (default: xrootd).\n"
      "If command contains -R <user>, that user is used and -R is stripped\n"
      "so xrootd does not drop the capabilities again.\n"
      "\n"
      "  sudo xrootd-fsuid -u xyz -- xrootd -c /etc/xrootd/xrootd-posix.cfg\n"
      "  sudo xrootd-fsuid xrootd -R xyz -c /etc/xrootd/xrootd-posix.cfg\n");
   exit(rc);
}

/* Pull -R user out of an xrootd-style argv. Returns a new argv (caller frees
   the array only). */
static char **StripDashR(int argc, char **argv, const char **userFromR, int *nargc)
{
   char **out = (char **)calloc((size_t)argc + 1, sizeof(char *));
   int n = 0, i;

   if (!out)
      {perror("calloc"); exit(2);}
   *userFromR = 0;
   for (i = 0; i < argc; i++)
       {if (!strcmp(argv[i], "-R") && i + 1 < argc)
           {if (!*userFromR) *userFromR = argv[++i];
            else i++;
            continue;
           }
        if (!strncmp(argv[i], "-R", 2) && argv[i][2])
           {if (!*userFromR) *userFromR = argv[i] + 2;
            continue;
           }
        out[n++] = argv[i];
       }
   out[n] = 0;
   *nargc = n;
   return out;
}

#ifdef __linux__
static int RaiseFsCaps(void)
{
   struct __user_cap_header_struct hdr;
   struct __user_cap_data_struct   data[2];
   unsigned int want;

   memset(&hdr, 0, sizeof(hdr));
   memset(data, 0, sizeof(data));
   hdr.version = _LINUX_CAPABILITY_VERSION_3;
   hdr.pid = 0;
   if (syscall(SYS_capget, &hdr, data) < 0) return -1;

   want = (1u << CAP_SETUID) | (1u << CAP_SETGID) | (1u << CAP_NET_BIND_SERVICE);
   data[0].permitted   |= want;
   data[0].effective   |= want;
   data[0].inheritable |= want;
   if (syscall(SYS_capset, &hdr, data) < 0) return -1;

   if (prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_RAISE, CAP_SETUID, 0, 0) < 0)
      return -1;
   if (prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_RAISE, CAP_SETGID, 0, 0) < 0)
      return -1;
   if (prctl(PR_CAP_AMBIENT, PR_CAP_AMBIENT_RAISE, CAP_NET_BIND_SERVICE, 0, 0) < 0)
      return -1;
   return 0;
}

static int DropKeepCaps(const struct passwd *pw)
{
   if (initgroups(pw->pw_name, pw->pw_gid) < 0) return -1;
   if (prctl(PR_SET_KEEPCAPS, 1L) < 0) return -1;
   if (setgid(pw->pw_gid) < 0) return -1;
   if (setuid(pw->pw_uid) < 0) return -1;
   if (RaiseFsCaps() < 0) return -1;
   if (prctl(PR_SET_KEEPCAPS, 0L) < 0) return -1;
   return 0;
}
#endif

int main(int argc, char **argv)
{
   const char *user = 0, *userR = 0;
   char **cmdv;
   int cmdc, argi = 1;

   while (argi < argc)
         {if (!strcmp(argv[argi], "-h") || !strcmp(argv[argi], "--help"))
             Usage(0);
          if (!strcmp(argv[argi], "-u") && argi + 1 < argc)
             {user = argv[++argi]; argi++; continue;}
          if (!strncmp(argv[argi], "-u", 2) && argv[argi][2])
             {user = argv[argi] + 2; argi++; continue;}
          if (!strcmp(argv[argi], "--"))
             {argi++; break;}
          break;
         }

   if (argi >= argc)
      {static char *dflt[] = { "xrootd", 0 };
       cmdv = dflt;
       cmdc = 1;
      }
      else
      {cmdv = argv + argi;
       cmdc = argc - argi;
      }

   cmdv = StripDashR(cmdc, cmdv, &userR, &cmdc);
   if (!user) user = userR;
   if (!user || !*user)
      {fprintf(stderr, "xrootd-fsuid: no user (-u or xrootd -R)\n");
       Usage(1);
      }
   if (cmdc < 1)
      {fprintf(stderr, "xrootd-fsuid: no command to exec\n");
       Usage(1);
      }

#ifndef __linux__
   (void)cmdv; (void)cmdc;
   fprintf(stderr, "xrootd-fsuid: Linux only\n");
   return 1;
#else
   int i;
   struct passwd *pw;

   if (geteuid() != 0)
      {fprintf(stderr, "xrootd-fsuid: must start as root (e.g. sudo)\n");
       return 2;
      }

   errno = 0;
   pw = getpwnam(user);
   if (!pw)
      {fprintf(stderr, "xrootd-fsuid: unknown user '%s'%s%s\n",
               user, errno ? ": " : "", errno ? strerror(errno) : "");
       return 2;
      }
   if (pw->pw_uid == 0)
      {fprintf(stderr, "xrootd-fsuid: refusing to target uid 0\n");
       return 2;
      }

   if (DropKeepCaps(pw) < 0)
      {fprintf(stderr, "xrootd-fsuid: cannot drop to %s with fsuid caps: %s\n",
               user, strerror(errno));
       return 3;
      }

   fprintf(stderr, "xrootd-fsuid: uid=%d gid=%d caps=SETUID,SETGID,NET_BIND exec",
           (int)pw->pw_uid, (int)pw->pw_gid);
   for (i = 0; i < cmdc; i++) fprintf(stderr, " %s", cmdv[i]);
   fprintf(stderr, "\n");

   execvp(cmdv[0], cmdv);
   fprintf(stderr, "xrootd-fsuid: exec %s: %s\n", cmdv[0], strerror(errno));
   return 127;
#endif
}
