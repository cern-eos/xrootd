# SPDX-License-Identifier: GPL-2.0
obj-m := xiofs.o
xiofs-y := super.o inode.o file.o http1.o http2.o session.o rdma.o

ccflags-y += -I$(src)

# RHEL backports VFS and page cache API changes without raising
# LINUX_VERSION_CODE (EL9.6 has the 6.3 mnt_idmap and the 5.19 folio
# interfaces on 5.14), so test the headers. Patterns use '.' for
# parentheses, which make would otherwise try to match.
xiofs_inc := $(srctree)/include/linux
xiofs_has = $(shell grep -qs '$(1)' $(xiofs_inc)/$(2) && echo -D$(3))

ccflags-y += $(call xiofs_has,struct mnt_idmap,fs.h,XIOFS_HAS_MNT_IDMAP)
ccflags-y += $(call xiofs_has,generic_fillattr.struct mnt_idmap \*. u32,fs.h,XIOFS_HAS_FILLATTR_MASK)
ccflags-y += $(call xiofs_has,read_folio..struct file,fs.h,XIOFS_HAS_READ_FOLIO)
ccflags-y += $(call xiofs_has,readahead_folio,pagemap.h,XIOFS_HAS_READAHEAD_FOLIO)
ccflags-y += $(call xiofs_has,dirty_folio..struct address_space,fs.h,XIOFS_HAS_DIRTY_FOLIO)
ccflags-y += $(call xiofs_has,writepage_t..struct folio,writeback.h,XIOFS_HAS_WRITEPAGE_FOLIO)
ccflags-y += $(call xiofs_has,struct dentry \*..mkdir.,fs.h,XIOFS_HAS_MKDIR_DENTRY)
ccflags-y += $(call xiofs_has,write_begin..const struct kiocb,fs.h,XIOFS_HAS_WRITE_BEGIN_KIOCB)
ccflags-y += $(shell grep -s -A1 '^struct page \*grab_cache_page_write_begin' \
	$(xiofs_inc)/pagemap.h | grep -qs 'pgoff_t index.;' && \
	echo -DXIOFS_HAS_WRITE_BEGIN_NOFLAGS)
