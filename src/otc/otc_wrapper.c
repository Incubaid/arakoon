/*
This file is part of Arakoon, a distributed key-value store. Copyright
(C) 2010 Incubaid BVBA

Licensees holding a valid Incubaid license may use this file in
accordance with Incubaid's Arakoon commercial license agreement. For
more information on how to enter into this agreement, please contact
Incubaid (contact details can be found on www.arakoon.org/licensing).

Alternatively, this file may be redistributed and/or modified under
the terms of the GNU Affero General Public License version 3, as
published by the Free Software Foundation. Under this license, this
file is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.

See the GNU Affero General Public License for more details.
You should have received a copy of the
GNU Affero General Public License along with this program (file "COPYING").
If not, see <http://www.gnu.org/licenses/>.
*/

#include <stdio.h>
#include <assert.h>
#include <string.h>

#include <caml/mlvalues.h>
#include <caml/bigarray.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/custom.h>
#include <caml/fail.h>

#include "tcbdb.h"

#define Some_val(v) Field(v,0)
#define Val_none Val_int(0)

// TODO: retrieve rich tc error for exceptions

static struct custom_operations bdb_ops = {
  "org.b-virtual.caml.tc.bdb",
  custom_finalize_default,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default
};

static struct custom_operations bdbcur_ops = {
  "org.b-virtual.caml.tc.bdbcur",
  custom_finalize_default,
  custom_compare_default,
  custom_hash_default,
  custom_serialize_default,
  custom_deserialize_default
};

/* Accessing the TCBDB * part of a Caml custom block */
#define Bdb_val(v) (*((TCBDB **) Data_custom_val(v)))
#define Bdbcur_val(v) (*((BDBCUR **) Data_custom_val(v)))

/* Allocating a Caml custom block to hold the given TCBDB * */
static value alloc_bdb(TCBDB * db)
{
  value v = alloc_custom(&bdb_ops, sizeof(TCBDB *), 0, 1);
  Bdb_val(v) = db;
  return v;
}

static value alloc_bdbcur(BDBCUR * cur)
{
  value v = alloc_custom(&bdbcur_ops, sizeof(BDBCUR *), 0, 1);
  Bdbcur_val(v) = cur;
  return v;
}

static inline void bdb_handle_error(TCBDB *bdb)
{
  const int ecode = tcbdbecode(bdb);
  if (ecode == TCENOREC) {
    caml_raise_not_found();
  } /* else if (ecode == TCEKEEP) {

} */ else {
    caml_failwith(tcbdberrmsg(ecode));
  }
}

static inline value caml_copy_string_with_length(const char *str, int len)
{
  value res = caml_alloc_string(len);
  memmove(String_val(res), str, len);
  return res;
}

value bdb_make(value unit)
{
  CAMLparam1(unit);
  CAMLreturn(alloc_bdb(tcbdbnew()));
}

void bdb_delete(value bdb)
{
  CAMLparam1(bdb);
  tcbdbdel(Bdb_val(bdb));
  CAMLreturn0;
}

void bdb_dbopen(value bdb, value filename, value mode)
{
  CAMLparam3(bdb, filename, mode);

  if (!tcbdbopen(Bdb_val(bdb), String_val(filename), Int_val(mode)))
    {
      bdb_handle_error(Bdb_val(bdb));
    }
  CAMLreturn0;
}

void bdb_dbclose(value bdb)
{
  CAMLparam1(bdb);
  if (!tcbdbclose(Bdb_val(bdb)))
    {
      bdb_handle_error(Bdb_val(bdb));
    }
  CAMLreturn0;
}

void bdb_dbsync(value bdb)
{
  CAMLparam1(bdb);
  if (!tcbdbsync(Bdb_val(bdb)))
    {
      bdb_handle_error(Bdb_val(bdb));
    }
  CAMLreturn0;
}

value bdb_cur_make(value bdb)
{
  CAMLparam1(bdb);
  CAMLreturn(alloc_bdbcur(tcbdbcurnew(Bdb_val(bdb))));
}

void bdb_cur_delete(value bdbcur) {
  CAMLparam1(bdbcur);
  tcbdbcurdel(Bdbcur_val(bdbcur));
  CAMLreturn0;
}



void bdb_first(value bdb, value bdbcur)
{
  CAMLparam2(bdb, bdbcur);
  if (!tcbdbcurfirst(Bdbcur_val(bdbcur)))
    {
      bdb_handle_error(Bdb_val(bdb));
    }
  CAMLreturn0;
}

void bdb_next(value bdb, value bdbcur)
{
  CAMLparam2(bdb, bdbcur);
  if (!tcbdbcurnext(Bdbcur_val(bdbcur)))
    {
      bdb_handle_error(Bdb_val(bdb));
    }
  CAMLreturn0;
}

void bdb_prev(value bdb, value bdbcur)
{
  CAMLparam2(bdb, bdbcur);
  if (!tcbdbcurprev(Bdbcur_val(bdbcur)))
    {
      bdb_handle_error(Bdb_val(bdb));
    }
  CAMLreturn0;
}

void bdb_last(value bdb, value bdbcur)
{
  CAMLparam2(bdb, bdbcur);
  if (!tcbdbcurlast(Bdbcur_val(bdbcur)))
    {
      bdb_handle_error(Bdb_val(bdb));
    }
  CAMLreturn0;
}

value bdb_key(value bdb, value bdbcur)
{
  CAMLparam2(bdb, bdbcur);
  CAMLlocal1(res);
  int len = 0;
  char *str =tcbdbcurkey(Bdbcur_val(bdbcur),&len);
  if (str == 0)
    {
      bdb_handle_error(Bdb_val(bdb));
    }
  res = caml_copy_string_with_length(str, len);
  free(str);
  CAMLreturn(res);
}

value bdb_value(value bdb, value bdbcur)
{
  CAMLparam2(bdb, bdbcur);
  CAMLlocal1(res);
  int len = 0;
  char *str =tcbdbcurval(Bdbcur_val(bdbcur), &len);
  if (str == 0)
    {
      bdb_handle_error(Bdb_val(bdb));
    }
  res = caml_copy_string_with_length(str, len);
  free(str);
  CAMLreturn(res);
}

value bdb_record(value bdb, value bdbcur)
{
  CAMLparam2(bdb, bdbcur);
  CAMLlocal3(res_key, res_value, res_tup);
  TCXSTR * key = tcxstrnew();
  TCXSTR * v1 = tcxstrnew();
  if (!tcbdbcurrec(Bdbcur_val(bdbcur), key, v1))
    {
      tcxstrdel(key);
      tcxstrdel(v1);
      bdb_handle_error(Bdb_val(bdb));
    }
  res_key = caml_copy_string_with_length(tcxstrptr(key), tcxstrsize(key));
  res_value = caml_copy_string_with_length(tcxstrptr(v1), tcxstrsize(v1));
  tcxstrdel(key);
  tcxstrdel(v1);
  res_tup = caml_alloc(2, 0);
  Store_field( res_tup, 0, res_key );
  Store_field( res_tup, 1, res_value );
  CAMLreturn(res_tup);
}

void bdb_jump(value bdb, value bdbcur, value key)
{
  CAMLparam3(bdb, bdbcur, key);
  if (!tcbdbcurjump2(Bdbcur_val(bdbcur), String_val(key)))
    {
      bdb_handle_error(Bdb_val(bdb));
    }
  CAMLreturn0;
}

void bdb_cur_put(value bdb, value bdbcur, value val, value option)
{
  CAMLparam4(bdb, bdbcur, val, option);
  const int vlen = caml_string_length(val);
  if (!tcbdbcurput(Bdbcur_val(bdbcur), String_val(val), vlen, Int_val(option)))
    {
      bdb_handle_error(Bdb_val(bdb));
    }
  CAMLreturn0;
}

void bdb_put(value bdb, value key, value val)
{
  CAMLparam3(bdb, key, val);
  const int klen = caml_string_length(key);
  const int vlen = caml_string_length(val);
  if (!tcbdbput(Bdb_val(bdb), String_val(key), klen, String_val(val), vlen))
    {
      bdb_handle_error(Bdb_val(bdb));
    }
  CAMLreturn0;
}

void bdb_putkeep(value bdb, value key, value val)
{
  CAMLparam3(bdb, key, val);
  const int klen = caml_string_length(key);
  const int vlen = caml_string_length(val);
  if (!tcbdbputkeep(Bdb_val(bdb), String_val(key), klen, String_val(val), vlen))
    {
      bdb_handle_error(Bdb_val(bdb));
    }
  CAMLreturn0;
}

value bdb_get(value bdb, value key)
{
  CAMLparam2(bdb, key);
  CAMLlocal1(res);
  const int klen = caml_string_length(key);
  int vlen;
  char * v1 = tcbdbget(Bdb_val(bdb), String_val(key), klen, &vlen);
  if (v1 == 0)
    {
      bdb_handle_error(Bdb_val(bdb));
    } else {
    res = caml_copy_string_with_length(v1, vlen);
    free(v1);
  }
  CAMLreturn(res);
}

void bdb_out(value bdb, value key)
{
  CAMLparam2(bdb, key);
  CAMLlocal1(res);
  const int klen = caml_string_length(key);
  bool ok = tcbdbout(Bdb_val(bdb), String_val(key), klen);
  if (!ok)
    {
      bdb_handle_error(Bdb_val(bdb));
    }
  CAMLreturn0;
}

void bdb_cur_out(value bdb, value bdbcur)
{
  CAMLparam2(bdb, bdbcur);
  if (!tcbdbcurout(Bdbcur_val(bdbcur)))
    {
      bdb_handle_error(Bdb_val(bdb));
    }
  CAMLreturn0;
}

void bdb_tranbegin(value bdb)
{
  CAMLparam1(bdb);
  if (!tcbdbtranbegin(Bdb_val(bdb)))
    {
      bdb_handle_error(Bdb_val(bdb));
    }
  CAMLreturn0;
}

void bdb_trancommit(value bdb)
{
  CAMLparam1(bdb);
  if (!tcbdbtrancommit(Bdb_val(bdb)))
    {
      bdb_handle_error(Bdb_val(bdb));
    }
  CAMLreturn0;
}

void bdb_tranabort(value bdb)
{
  CAMLparam1(bdb);
  if (!tcbdbtranabort(Bdb_val(bdb)))
    {
      bdb_handle_error(Bdb_val(bdb));
    }
  CAMLreturn0;
}

value bdb_range_native(
		       value bdb,
		       value bk, value binc,
		       value ek, value einc,
		       value maxnum)
{
  CAMLparam1(bdb);
  CAMLxparam5(bk,binc,ek,einc,maxnum);
  CAMLlocal1(res);
  int blen = 0;
  int elen = 0;
  char * bks = NULL;
  char * eks = NULL;
  if (bk != Val_none) {
    value x = Some_val(bk);
    bks = String_val(x);
    blen = caml_string_length(x);
  }
  if (ek != Val_none) {
    value x = Some_val(ek);
    eks = String_val(x);
    elen = caml_string_length(x);
  }
  const int maxnumi = Int_val(maxnum);
  const int binci = Bool_val(binc);
  const int einci = Bool_val(einc);
  /*fprintf(stderr, "bdb_range: maxnum: %d bks:%s binc:%d eks:%s einc:%d\n",
    maxnumi, bks, binci, eks, einci); */
  TCLIST *l = tcbdbrange(Bdb_val(bdb),
			  bks, blen, binci,
			  eks, elen, einci,
			  maxnumi);
  int n = tclistnum(l);
  int i;
  res = caml_alloc(n, 0);
  for (i = 0; i < n; i++)
    {
      int vlen;
      const char * v = tclistval(l, i, &vlen);
      value v2 = caml_copy_string_with_length(v, vlen);
      caml_modify(&Field(res, i), v2);
    }
  tclistdel(l);
  CAMLreturn(res);
}

value bdb_prefix_keys(value bdb, value prefix, value maxnum)
{
  CAMLparam3(bdb, prefix, maxnum);
  CAMLlocal1(res);
  const int plen = caml_string_length(prefix);
  TCLIST *l = tcbdbfwmkeys(Bdb_val(bdb),
			   String_val(prefix),
			   plen,
			   Int_val(maxnum)
			   );
  int n = tclistnum(l);
  int i;
  res = caml_alloc(n, 0);
  for (i = 0; i < n; i++)
    {
      int vlen;
      const char * v = tclistval(l, i, &vlen);
      value v2 = caml_copy_string_with_length(v, vlen);
      caml_modify(&Field(res, i), v2);
    }
  tclistdel(l);
  CAMLreturn(res);
}

CAMLprim value
bdb_range_bytecode( value * argv, int argn )
{
  (void) argn;
  return bdb_range_native( argv[0], argv[1], argv[2],
			   argv[3], argv[4], argv[5] );
}


value bdb_optimize(value bdb) 
{
  CAMLparam1(bdb);
  int res = tcbdboptimize(Bdb_val(bdb), 0, 0, 0, -1, -1, UINT8_MAX);
  CAMLreturn(res?Val_true:Val_false);
}

value bdb_defrag(value bdb){
  CAMLparam1(bdb);
  //printf("otc_wrapper: bdb_defrag\n");
  TCBDB* tcbdb = Bdb_val(bdb);
  int res = !tcbdbdefrag(tcbdb, INT64_MAX);
  //printf("otc_wrapper: bdb_defrag=>%i\n",res);
  if (res){
    bdb_handle_error(tcbdb);
  }
  CAMLreturn(Val_int(res));
}

value bdb_key_count(value bdb)
{
  CAMLparam1(bdb);
  uint64_t count = tcbdbrnum( Bdb_val(bdb) );
  CAMLreturn ( copy_int64(count) );
}
